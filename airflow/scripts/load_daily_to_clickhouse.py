from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timedelta
import logging
import requests
import json
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session():
    return SparkSession.builder \
        .appName("DailySilverLayer") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def load_daily_parquet(date, market, symbol):
    """Load single day's parquet file"""
    spark = get_spark_session()
    
    try:
        base_path = "/Users/iraklikereleishvili/Documents/github/CryptoBridge-TH-Global/data/raw"
        file_path = f"{base_path}/{market}_{symbol}_{date.strftime('%Y%m%d')}.parquet"
        
        df = spark.read.parquet(file_path)
        logger.info(f"Loaded {market} {symbol} data for {date}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load {market} {symbol} data: {e}")
        return None

def process_market_data(date, market_data):
    """Process market data for a single day"""
    if not market_data:
        return None
        
    spark = get_spark_session()
    
    try:
        # Convert to DataFrame and add metadata
        df = spark.createDataFrame(market_data)
        
        # Add metadata columns
        df = df.withColumn("date", lit(date.strftime('%Y-%m-%d'))) \
             .withColumn("processed_at", current_timestamp())
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to process market data: {e}")
        return None

def save_to_clickhouse(df, date):
    """Save processed data to ClickHouse"""
    if df is None:
        return
        
    try:
        # Convert to records for insertion
        records = df.toPandas().to_dict('records')
        
        # Delete existing data for the date if any
        delete_query = f"ALTER TABLE cryptobridge.crypto_aggregates DELETE WHERE date = '{date.strftime('%Y-%m-%d')}'"
        requests.post(
            'http://localhost:8123',
            params={'query': delete_query},
            headers={'Content-Type': 'application/json'},
            json={'user': 'analytics', 'password': 'analytics123'}
        ).raise_for_status()
        
        # Insert new data
        requests.post(
            'http://localhost:8123',
            params={'query': 'INSERT INTO cryptobridge.crypto_aggregates FORMAT JSONEachRow'},
            headers={'Content-Type': 'application/json'},
            data='\n'.join(json.dumps(record) for record in records),
            json={'user': 'analytics', 'password': 'analytics123'}
        ).raise_for_status()
        
        logger.info(f"Successfully saved data for {date} to ClickHouse")
        
    except Exception as e:
        logger.error(f"Failed to save to ClickHouse: {e}")
        raise

def main(execution_date=None):
    if execution_date is None:
        execution_date = datetime.now().date() - timedelta(days=1)
    elif isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    
    logger.info(f"Processing data for {execution_date}")
    
    try:
        market_data = []
        
        # Process global market data
        for symbol in ["BTCUSDT", "ETHUSDT", "USDCUSDT"]:
            df = load_daily_parquet(execution_date, "global", symbol)
            if df is not None:
                market_data.append({
                    "market": "global",
                    "coin": symbol.replace("USDT", ""),
                    "data": df
                })
        
        # Process Thai market data
        for symbol in ["THB_BTC", "THB_ETH", "THB_USDT"]:
            df = load_daily_parquet(execution_date, "th", symbol)
            if df is not None:
                market_data.append({
                    "market": "th",
                    "coin": symbol.split("_")[1],
                    "data": df
                })
        
        if not market_data:
            raise Exception(f"No data found for {execution_date}")
        
        # Process and combine all market data
        processed_df = process_market_data(execution_date, market_data)
        
        # Save to ClickHouse
        if processed_df is not None:
            save_to_clickhouse(processed_df, execution_date)
        
    except Exception as e:
        logger.error(f"Failed to process data for {execution_date}: {e}")
        raise
    finally:
        spark = SparkSession._instantiatedSession
        if spark:
            spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process daily crypto data for silver layer')
    parser.add_argument('--execution_date', type=str, help='Execution date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    main(args.execution_date)