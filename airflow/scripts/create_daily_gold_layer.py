from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, sum, lit, round,
    first, coalesce
)
import logging
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session():
    return SparkSession.builder \
        .appName("CryptoDailyGoldLayer") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def load_daily_data(date):
    """Load single day's data from silver layer"""
    query = f"""
    SELECT *
    FROM cryptobridge.crypto_aggregates
    WHERE date = '{date.strftime('%Y-%m-%d')}'
    FORMAT JSONEachRow
    """
    
    try:
        response = requests.post(
            'http://localhost:8123',
            params={'query': query},
            headers={'Content-Type': 'application/json'},
            json={'user': 'analytics', 'password': 'analytics123'}
        )
        response.raise_for_status()
        
        data = [json.loads(line) for line in response.text.strip().split('\n') if line]
        if not data:
            logger.warning(f"No data found for date {date}")
            return None
            
        df_pd = pd.DataFrame(data)
        spark = get_spark_session()
        return spark.createDataFrame(df_pd)
        
    except Exception as e:
        logger.error(f"Failed to read from ClickHouse: {e}")
        return None

def create_daily_summary(df):
    """Transform daily data to gold format"""
    daily_summary = df.groupBy("date").pivot("coin").agg(
        first(when(col("market") == "global", col("total_volume")).otherwise(None)).alias("volume_global"),
        first(when(col("market") == "global", col("total_quote_volume")).otherwise(None)).alias("quote_volume_global"),
        first(when(col("market") == "global", col("avg_price_usd")).otherwise(None)).alias("price_usd"),
        first(when(col("market") == "th", col("avg_price_thb")).otherwise(None)).alias("price_thb"),
        first(when(col("market") == "global", col("total_trades")).otherwise(None)).alias("trades_global")
    )

    # Calculate additional metrics
    for coin in ["BTC", "ETH", "USDC"]:
        daily_summary = daily_summary.withColumn(
            f"{coin}_quote_volume_global",
            coalesce(col(f"{coin}_quote_volume_global"), lit(0))
        )

    return daily_summary.withColumn(
        "total_trading_volume_usd",
        col("BTC_quote_volume_global") + 
        col("ETH_quote_volume_global") + 
        col("USDC_quote_volume_global")
    ).withColumn(
        "btc_dominance",
        round(col("BTC_quote_volume_global") / col("total_trading_volume_usd") * 100, 2)
    ).withColumn(
        "eth_dominance",
        round(col("ETH_quote_volume_global") / col("total_trading_volume_usd") * 100, 2)
    )

def save_to_clickhouse(df, date):
    """Save daily summary to ClickHouse"""
    try:
        # Convert to pandas for easier handling
        pandas_df = df.toPandas()
        
        # Delete existing data for the date if any
        delete_query = f"ALTER TABLE cryptobridge.crypto_daily_summary DELETE WHERE date = '{date.strftime('%Y-%m-%d')}'"
        requests.post(
            'http://localhost:8123',
            params={'query': delete_query},
            headers={'Content-Type': 'application/json'},
            json={'user': 'analytics', 'password': 'analytics123'}
        ).raise_for_status()
        
        # Insert new data
        records = pandas_df.to_dict('records')
        requests.post(
            'http://localhost:8123',
            params={'query': 'INSERT INTO cryptobridge.crypto_daily_summary FORMAT JSONEachRow'},
            headers={'Content-Type': 'application/json'},
            data='\n'.join(json.dumps(record) for record in records),
            json={'user': 'analytics', 'password': 'analytics123'}
        ).raise_for_status()
        
        logger.info(f"Successfully saved daily summary for {date}")
        
    except Exception as e:
        logger.error(f"Failed to save to ClickHouse: {e}")
        raise

def main(execution_date=None):
    """Process single day's data"""
    if execution_date is None:
        execution_date = datetime.now().date() - timedelta(days=1)
    elif isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    
    logger.info(f"Processing data for {execution_date}")
    
    try:
        # Load data
        df = load_daily_data(execution_date)
        if df is None:
            raise Exception(f"No data available for {execution_date}")
        
        # Create daily summary
        daily_summary = create_daily_summary(df)
        
        # Save to ClickHouse
        save_to_clickhouse(daily_summary, execution_date)
        
    except Exception as e:
        logger.error(f"Failed to process data for {execution_date}: {e}")
        raise
    finally:
        # Clean up Spark session
        spark = SparkSession._instantiatedSession
        if spark:
            spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create daily gold layer summary')
    parser.add_argument('--execution_date', type=str, help='Execution date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    main(args.execution_date)