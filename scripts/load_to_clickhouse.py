from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit, avg, sum
from pyspark.sql.types import DoubleType
import logging
import pandas as pd
import requests
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("CryptoToClickHouse") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

symbols = ['BTCUSDT', 'ETHUSDT', 'USDCUSDT', 'BTCTHB', 'ETHTHB', 'USDTTHB']
dfs = []
for symbol in symbols:
    market = 'global' if symbol in ['BTCUSDT', 'ETHUSDT', 'USDCUSDT'] else 'th'
    try:
        df = spark.read.parquet(f"hdfs://localhost:9000/data_crypto/raw_{market}_{symbol}.parquet")
        df = df.withColumn("volume", col("volume").cast(DoubleType())) \
               .withColumn("quote_volume", col("quote_volume").cast(DoubleType())) \
               .withColumn("close", col("close").cast(DoubleType())) \
               .withColumn("num_trades", col("num_trades").cast("integer"))
        
        df = df.withColumn("symbol", lit(symbol)).withColumn("market", lit(market))
        dfs.append(df)
        logger.info(f"Loaded {symbol} from HDFS with {df.count()} rows")
    except Exception as e:
        logger.error(f"Failed to load {symbol}: {e}")
        continue

if dfs:
    raw_df = dfs[0]
    for df in dfs[1:]:
        raw_df = raw_df.union(df)
else:
    logger.error("No data loaded from HDFS")
    spark.stop()
    exit(1)

# Aggregate data daily
transformed_df = raw_df \
    .withColumn("date", to_date(col("open_time"))) \
    .withColumn("coin", when(col("symbol").isin("BTCUSDT", "BTCTHB"), "BTC")
                       .when(col("symbol").isin("ETHUSDT", "ETHTHB"), "ETH")
                       .when(col("symbol").isin("USDCUSDT", "USDTTHB"), "USDC")
                       .otherwise("UNKNOWN")) \
    .groupBy("date", "coin", "market") \
    .agg(
        avg(when(col("market") == "th", col("close")).otherwise(None)).alias("avg_price_thb"),
        avg(when(col("market") == "global", col("close")).otherwise(None)).alias("avg_price_usd"),
        sum("volume").alias("total_volume"),
        sum("quote_volume").alias("total_quote_volume"),
        sum("num_trades").alias("total_trades")
    )

logger.info(f"Transformed data has {transformed_df.count()} rows")
transformed_df.show(5)

# Convert to Pandas DataFrame
pandas_df = transformed_df.toPandas()

# Prepare data for ClickHouse INSERT
data = []
for _, row in pandas_df.iterrows():
    data.append({
        'date': row['date'].strftime('%Y-%m-%d'),
        'coin': row['coin'],
        'market': row['market'],
        'total_volume': float(row['total_volume']),
        'total_quote_volume': float(row['total_quote_volume']),
        'avg_price_thb': float(row['avg_price_thb']) if pd.notna(row['avg_price_thb']) else None,
        'avg_price_usd': float(row['avg_price_usd']) if pd.notna(row['avg_price_usd']) else None,
        'total_trades': int(row['total_trades'])
    })

# Insert data using ClickHouse HTTP interface
try:
    for chunk in [data[i:i + 1000] for i in range(0, len(data), 1000)]:
        response = requests.post(
            'http://localhost:8123',
            params={
                'query': f'INSERT INTO cryptobridge.crypto_aggregates FORMAT JSONEachRow',
                'user': 'analytics',
                'password': 'analytics123'
            },
            data='\n'.join(json.dumps(row) for row in chunk)
        )
        response.raise_for_status()
    logger.info("Data successfully written to ClickHouse table crypto_aggregates")
except Exception as e:
    logger.error(f"Failed to write to ClickHouse: {e}")

spark.stop()