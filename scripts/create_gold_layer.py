from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col as F_col,  # Rename to avoid conflict
    when, sum, lit, round, expr, 
    first, concat, format_string, coalesce,
    avg, count, min, max, stddev
)
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, DateType
import logging
from datetime import datetime, timedelta
import requests
import pandas as pd
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_data_quality(df, table_name):
    """Validate data quality and log metrics"""
    logger.info(f"\nData Quality Report for {table_name}:")
    
    # Count nulls per column
    for column_name in df.columns:
        null_count = df.filter(F_col(column_name).isNull()).count()
        total_count = df.count()
        null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
        logger.info(f"Column {column_name}: {null_count} nulls ({null_percentage:.2f}%)")
    
    # Show basic statistics using Spark's built-in summary
    logger.info("\nBasic Statistics:")
    df.summary().show()
    
    return df

def clean_data(df):
    """Clean and prepare data"""
    logger.info("Starting data cleaning process...")
    
    # 1. Handle missing values
    df_cleaned = df.na.fill({
        'total_volume': 0,
        'total_quote_volume': 0,
        'total_trades': 0
    })
    
    # 2. Remove any rows with invalid dates
    current_date = datetime.now().date()
    df_cleaned = df_cleaned.filter(
        (F_col('date').isNotNull()) & 
        (F_col('date') <= current_date)
    )
    
    # 3. Handle outliers for volume and trades
    # Calculate statistics for outlier detection
    stats = df_cleaned.select([
        avg('total_volume').alias('avg_volume'),
        stddev('total_volume').alias('stddev_volume'),
        avg('total_trades').alias('avg_trades'),
        stddev('total_trades').alias('stddev_trades')
    ]).collect()[0]
    
    # Define outlier thresholds (3 standard deviations)
    volume_upper = stats['avg_volume'] + 3 * stats['stddev_volume']
    trades_upper = stats['avg_trades'] + 3 * stats['stddev_trades']
    
    # Cap outliers at threshold values
    df_cleaned = df_cleaned.withColumn(
        'total_volume',
        when(F_col('total_volume') > volume_upper, volume_upper)
        .otherwise(F_col('total_volume'))
    ).withColumn(
        'total_trades',
        when(F_col('total_trades') > trades_upper, trades_upper)
        .otherwise(F_col('total_trades'))
    )
    
    logger.info("Data cleaning completed")
    return df_cleaned

# Initialize Spark
spark = SparkSession.builder \
    .appName("CryptoGoldLayer") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Read from ClickHouse using HTTP interface
logger.info("Reading silver layer data from ClickHouse...")
try:
    # Query ClickHouse via HTTP
    response = requests.get(
        'http://localhost:8123',
        params={
            'query': 'SELECT * FROM cryptobridge.crypto_aggregates FORMAT JSONEachRow',
            'user': 'analytics',
            'password': 'analytics123'
        }
    )
    response.raise_for_status()
    
    # Convert response to pandas DataFrame
    data = [json.loads(line) for line in response.text.strip().split('\n') if line]
    df_pd = pd.DataFrame(data)
    
    # Convert date string to datetime
    df_pd['date'] = pd.to_datetime(df_pd['date']).dt.date
    
    # Convert numeric columns
    numeric_columns = ['total_volume', 'total_quote_volume', 'avg_price_thb', 'avg_price_usd', 'total_trades']
    for col in numeric_columns:
        df_pd[col] = pd.to_numeric(df_pd[col], errors='coerce')
    
    # Convert to Spark DataFrame
    silver_df = spark.createDataFrame(df_pd)
    logger.info(f"Successfully loaded {silver_df.count()} rows from ClickHouse")
    
except Exception as e:
    logger.error(f"Failed to read from ClickHouse: {e}")
    spark.stop()
    exit(1)

# Validate input data quality
silver_df = validate_data_quality(silver_df, "Silver Layer Input")

# Clean the data
silver_df_cleaned = clean_data(silver_df)

# Create pivot and aggregations for gold layer
logger.info("Creating gold layer transformations...")
gold_df = silver_df_cleaned \
    .groupBy("date") \
    .pivot("coin") \
    .agg(
        first(when(F_col("market") == "global", F_col("total_volume")).otherwise(None)).alias("volume_global"),
        first(when(F_col("market") == "global", F_col("total_quote_volume")).otherwise(None)).alias("quote_volume_global"),
        first(when(F_col("market") == "global", F_col("avg_price_usd")).otherwise(None)).alias("price_usd"),
        first(when(F_col("market") == "th", F_col("avg_price_thb")).otherwise(None)).alias("price_thb"),
        first(when(F_col("market") == "global", F_col("total_trades")).otherwise(None)).alias("trades_global")
    )

# Handle null values in aggregated data
for coin in ["BTC", "ETH", "USDC"]:
    for metric in ["volume_global", "quote_volume_global", "trades_global"]:
        gold_df = gold_df.withColumn(
            f"{coin}_{metric}",
            coalesce(F_col(f"{coin}_{metric}"), lit(0))
        )

# Calculate total volumes and market dominance
gold_df = gold_df \
    .withColumn("total_trading_volume_usd", 
        coalesce(F_col("BTC_quote_volume_global"), lit(0)) + 
        coalesce(F_col("ETH_quote_volume_global"), lit(0)) + 
        coalesce(F_col("USDC_quote_volume_global"), lit(0))) \
    .withColumn("total_trades",
        coalesce(F_col("BTC_trades_global"), lit(0)) + 
        coalesce(F_col("ETH_trades_global"), lit(0)) + 
        coalesce(F_col("USDC_trades_global"), lit(0))) \
    .withColumn("btc_dominance", 
        round(coalesce(F_col("BTC_quote_volume_global"), lit(0)) / 
              when(F_col("total_trading_volume_usd") > 0, F_col("total_trading_volume_usd")).otherwise(1) * 100, 2)) \
    .withColumn("eth_dominance", 
        round(coalesce(F_col("ETH_quote_volume_global"), lit(0)) / 
              when(F_col("total_trading_volume_usd") > 0, F_col("total_trading_volume_usd")).otherwise(1) * 100, 2))

# Validate output data quality
gold_df = validate_data_quality(gold_df, "Gold Layer Output")

# Show sample of the transformed data
logger.info("Sample of gold layer data:")
gold_df.show(5)

# Write to ClickHouse gold layer
try:
    # Convert Spark DataFrame to Pandas and clean data
    pandas_df = gold_df.toPandas()
    
    # 1. Convert date to string YYYY-MM-DD format
    pandas_df['date'] = pandas_df['date'].astype(str)
    
    # 2. Convert float columns to simple numeric format without scientific notation
    float_columns = [
        'BTC_volume_global', 'BTC_quote_volume_global', 'BTC_price_usd', 'BTC_price_thb',
        'ETH_volume_global', 'ETH_quote_volume_global', 'ETH_price_usd', 'ETH_price_thb',
        'USDC_volume_global', 'USDC_quote_volume_global', 'USDC_price_usd', 'USDC_price_thb',
        'total_trading_volume_usd', 'btc_dominance', 'eth_dominance'
    ]
    
    # 3. Convert integer columns
    int_columns = ['BTC_trades_global', 'ETH_trades_global', 'USDC_trades_global', 'total_trades']
    
    # 4. Fill NaN values with 0 and ensure proper types
    for col in float_columns:
        pandas_df[col] = pandas_df[col].fillna(0).astype(float)
    
    for col in int_columns:
        pandas_df[col] = pandas_df[col].fillna(0).astype(int)
    
    # 5. Prepare records for insertion
    records = pandas_df.to_dict('records')
    
    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cryptobridge.crypto_daily_summary (
        date Date,
        BTC_volume_global Float64,
        BTC_quote_volume_global Float64,
        BTC_price_usd Float64,
        BTC_price_thb Nullable(Float64),
        BTC_trades_global UInt32,
        ETH_volume_global Float64,
        ETH_quote_volume_global Float64,
        ETH_price_usd Float64,
        ETH_price_thb Nullable(Float64),
        ETH_trades_global UInt32,
        USDC_volume_global Float64,
        USDC_quote_volume_global Float64,
        USDC_price_usd Float64,
        USDC_price_thb Nullable(Float64),
        USDC_trades_global UInt32,
        total_trading_volume_usd Float64,
        total_trades UInt32,
        btc_dominance Float64,
        eth_dominance Float64
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY date
    """
    
    response = requests.post(
        'http://localhost:8123',
        params={
            'query': create_table_query,
            'user': 'analytics',
            'password': 'analytics123'
        }
    )
    response.raise_for_status()
    
    # Insert data in chunks
    chunk_size = 1000
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        
        # Print first record for debugging
        if i == 0:
            logger.info("Sample record being sent to ClickHouse:")
            logger.info(json.dumps(chunk[0], indent=2))
        
        try:
            # Simple JSON conversion without any special formatting
            data = '\n'.join(json.dumps(record) for record in chunk)
            response = requests.post(
                'http://localhost:8123',
                params={
                    'query': 'INSERT INTO cryptobridge.crypto_daily_summary FORMAT JSONEachRow',
                    'user': 'analytics',
                    'password': 'analytics123'
                },
                data=data
            )
            response.raise_for_status()
            # Print error response if any
            if response.status_code != 200:
                logger.error(f"Response text: {response.text}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Error response: {e.response.text}")
            raise
    
    logger.info("Successfully wrote gold layer data to ClickHouse")

except Exception as e:
    logger.error(f"Failed to write to ClickHouse: {e}")

spark.stop()