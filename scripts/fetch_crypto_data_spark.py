from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, IntegerType, TimestampType
import requests
import json
from datetime import datetime, timedelta
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CryptoDataIngestion") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# API Endpoints
TH_API = "https://api.binance.th/api/v1/klines"
GLOBAL_API = "https://api.binance.com/api/v3/klines"

# Symbols
TH_SYMBOLS = ['BTCTHB', 'ETHTHB', 'USDTTHB']
GLOBAL_SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'USDCUSDT']

# Schema for kline data
schema = StructType([
    StructField("open_time", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("close_time", TimestampType(), True),
    StructField("quote_volume", DoubleType(), True),
    StructField("num_trades", IntegerType(), True),
    StructField("taker_buy_base", DoubleType(), True),
    StructField("taker_buy_quote", DoubleType(), True),
    StructField("ignore", DoubleType(), True)
])

def fetch_klines(api_url, symbol, interval='1d', limit=1000, start_time=None, end_time=None):
    data = []
    current_start = start_time
    max_interval_ms = 7 * 24 * 60 * 60 * 1000  # 7 days in milliseconds
    
    logger.info(f"Fetching {symbol} from {api_url} for {datetime.fromtimestamp(start_time/1000)} to {datetime.fromtimestamp(end_time/1000)}")
    
    while current_start < end_time:
        current_end = min(current_start + max_interval_ms - 1, end_time)
        params = {'symbol': symbol, 'interval': interval, 'limit': limit, 'startTime': current_start, 'endTime': current_end}
        
        try:
            response = requests.get(api_url, params=params, timeout=10)
            response.raise_for_status()  # Raises HTTPError for bad status codes
            
            batch = response.json()
            if isinstance(batch, dict) and 'code' in batch:
                logger.error(f"API error for {symbol}: {batch}")
                break
            if not batch:
                logger.warning(f"No data returned for {symbol} from {datetime.fromtimestamp(current_start/1000)} to {datetime.fromtimestamp(current_end/1000)}")
                current_start = current_end + 1
                continue
            
            data.extend(batch)
            if batch:
                last_time = batch[-1][0]
                current_start = last_time + 1
                logger.info(f"Fetched {len(batch)} records for {symbol} from {datetime.fromtimestamp(current_start/1000)} to {datetime.fromtimestamp(current_end/1000)}")
            else:
                current_start = current_end + 1
            
            time.sleep(0.5)  # Avoid rate limits
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching {symbol} from {api_url}: {e}")
            current_start = current_end + 1
            time.sleep(2)  # Wait longer on network errors
            continue
        except Exception as e:
            logger.error(f"Unexpected error fetching {symbol} from {api_url}: {e}")
            current_start = current_end + 1
            continue
    
    for row in data:
        try:
            row[0] = datetime.fromtimestamp(row[0] / 1000.0)
            row[6] = datetime.fromtimestamp(row[6] / 1000.0)
            row[1:6] = [float(x) for x in row[1:6]]
            row[7] = float(row[7])
            row[8] = int(row[8])
            row[9] = float(row[9])
            row[10] = float(row[10])
            row[11] = float(row[11])
        except Exception as e:
            logger.error(f"Error processing row for {symbol}: {row}, {e}")
            continue
    
    logger.info(f"Total records fetched for {symbol}: {len(data)}")
    return data

def fetch_and_save(symbols, api_url, market_type):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)  # 1 year
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    
    logger.info(f"Processing {market_type} symbols: {symbols}")
    for symbol in symbols:
        logger.info(f"Processing {symbol} from {market_type}")
        try:
            raw_data = fetch_klines(api_url, symbol, start_time=start_time, end_time=end_time)
            
            if raw_data:
                df = spark.createDataFrame(raw_data, schema)
                hdfs_path = f"hdfs://localhost:9000/data_crypto/raw_{market_type}_{symbol}.parquet"
                df.write.mode("overwrite").parquet(hdfs_path)
                logger.info(f"Saved raw data to {hdfs_path}")
            else:
                logger.warning(f"No data fetched for {symbol} from {market_type}")
        except Exception as e:
            logger.error(f"Failed to process {symbol} from {market_type}: {e}")
            continue

try:
    fetch_and_save(GLOBAL_SYMBOLS, GLOBAL_API, 'global')
    time.sleep(2)  # Delay to avoid rate limits between APIs
    fetch_and_save(TH_SYMBOLS, TH_API, 'th')
except Exception as e:
    logger.error(f"Error in main execution: {e}")
finally:
    logger.info("Stopping Spark session")
    spark.stop()