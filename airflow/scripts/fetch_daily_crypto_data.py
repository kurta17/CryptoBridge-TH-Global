from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta
import requests
import json
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session():
    return SparkSession.builder \
        .appName("DailyCryptoDataFetch") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def fetch_binance_data(symbol, date):
    """Fetch single day's data from Binance"""
    start_ts = int(datetime.combine(date, datetime.min.time()).timestamp() * 1000)
    end_ts = int(datetime.combine(date + timedelta(days=1), datetime.min.time()).timestamp() * 1000)
    
    url = f"https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": "1d",
        "startTime": start_ts,
        "endTime": end_ts,
        "limit": 1
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            logger.warning(f"No data found for {symbol} on {date}")
            return None
            
        return {
            "open_time": data[0][0],
            "open": float(data[0][1]),
            "high": float(data[0][2]),
            "low": float(data[0][3]),
            "close": float(data[0][4]),
            "volume": float(data[0][5]),
            "close_time": data[0][6],
            "quote_volume": float(data[0][7]),
            "trades": int(data[0][8])
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch {symbol} data: {e}")
        return None

def fetch_bitkub_data(symbol, date):
    """Fetch single day's data from Bitkub"""
    start_ts = int(datetime.combine(date, datetime.min.time()).timestamp())
    end_ts = int(datetime.combine(date + timedelta(days=1), datetime.min.time()).timestamp())
    
    url = f"https://api.bitkub.com/api/market/tradingview"
    params = {
        "symbol": symbol,
        "resolution": "D",
        "from": start_ts,
        "to": end_ts
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('s') == 'ok' or not data.get('t'):
            logger.warning(f"No data found for {symbol} on {date}")
            return None
            
        idx = -1  # Get the last (and should be only) candle
        return {
            "timestamp": data['t'][idx] * 1000,  # Convert to milliseconds
            "open": float(data['o'][idx]),
            "high": float(data['h'][idx]),
            "low": float(data['l'][idx]),
            "close": float(data['c'][idx]),
            "volume": float(data['v'][idx])
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch {symbol} data: {e}")
        return None

def save_to_parquet(data_dict, date, market, symbol):
    """Save daily data to parquet file"""
    if not data_dict:
        return
        
    spark = get_spark_session()
    
    try:
        df = spark.createDataFrame([data_dict])
        
        # Save to appropriate path
        base_path = "/Users/iraklikereleishvili/Documents/github/CryptoBridge-TH-Global/data/raw"
        file_path = f"{base_path}/{market}_{symbol}_{date.strftime('%Y%m%d')}.parquet"
        
        df.write.mode("overwrite").parquet(file_path)
        logger.info(f"Saved {market} {symbol} data for {date} to {file_path}")
        
    except Exception as e:
        logger.error(f"Failed to save {market} {symbol} data: {e}")
        raise
    finally:
        spark.stop()

def main(execution_date=None):
    if execution_date is None:
        execution_date = datetime.now().date() - timedelta(days=1)
    elif isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    
    logger.info(f"Fetching data for {execution_date}")
    
    # Global market pairs
    global_pairs = [
        ("BTCUSDT", "BTC"),
        ("ETHUSDT", "ETH"),
        ("USDCUSDT", "USDC")
    ]
    
    # Thai market pairs
    th_pairs = [
        ("THB_BTC", "BTC"),
        ("THB_ETH", "ETH"),
        ("THB_USDT", "USDT")
    ]
    
    try:
        # Fetch global market data
        for symbol, base_asset in global_pairs:
            data = fetch_binance_data(symbol, execution_date)
            if data:
                save_to_parquet(data, execution_date, "global", symbol)
        
        # Fetch Thai market data
        for symbol, base_asset in th_pairs:
            data = fetch_bitkub_data(symbol, execution_date)
            if data:
                save_to_parquet(data, execution_date, "th", symbol)
                
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch daily crypto market data')
    parser.add_argument('--execution_date', type=str, help='Execution date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    main(args.execution_date)