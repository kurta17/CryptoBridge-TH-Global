#!/usr/bin/env python3
"""
🥉 Bronze Layer - Raw Data Ingestion Pipeline
Kafka → HDFS (Parquet) - Medallion Architecture

This pipeline ingests raw transaction data from Kafka topics and stores them
in the Bronze layer (HDFS) as Parquet files with minimal transformation.

🎯 Bronze Layer Features:
- 📥 Raw data ingestion from Kafka
- 🗃️ Parquet storage in HDFS
- 📊 Partitioned by date and topic
- 🔄 Exactly-once processing
- 📝 Schema evolution support
"""

import os
import sys
from datetime import datetime
from typing import Dict, Any
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    import json
except ImportError as e:
    print("❌ Missing PySpark dependencies!")
    print("Please install: pip install pyspark")
    print(f"Error: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeLayerProcessor:
    def __init__(self):
        """Initialize Bronze Layer Processor"""
        self.spark = None
        self.hdfs_base_path = "hdfs://localhost:9001/cryptobridge/bronze"
        self.kafka_servers = "localhost:9092"
        self.checkpoint_location = "hdfs://localhost:9001/checkpoints/bronze"
        
    def create_spark_session(self):
        """Create Spark session with optimized configurations"""
        try:
            self.spark = SparkSession.builder \
                .appName("CryptoBridge-Bronze-Layer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.sql.warehouse.dir", "hdfs://localhost:9001/warehouse") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9001") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark session created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            return False
    
    def define_schema(self):
        """Define schema for transaction data"""
        return StructType([
            StructField("transaction_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("sender", StructType([
                StructField("user_id", StringType(), True),
                StructField("account_type", StringType(), True),
                StructField("email", StringType(), True),
                StructField("country", StringType(), True),
                StructField("kyc_level", IntegerType(), True),
                StructField("preferred_crypto", StringType(), True)
            ]), True),
            StructField("receiver", StructType([
                StructField("user_id", StringType(), True),
                StructField("email", StringType(), True),
                StructField("country", StringType(), True),
                StructField("exchange", StringType(), True),
                StructField("kyc_level", IntegerType(), True)
            ]), True),
            StructField("transfer", StructType([
                StructField("crypto_symbol", StringType(), True),
                StructField("crypto_amount", DoubleType(), True),
                StructField("usd_equivalent", DoubleType(), True),
                StructField("thb_equivalent", DoubleType(), True),
                StructField("exchange_rate_usd_thb", DoubleType(), True)
            ]), True),
            StructField("fees", StructType([
                StructField("fee_usd", DoubleType(), True),
                StructField("fee_rate", DoubleType(), True),
                StructField("fee_type", StringType(), True)
            ]), True),
            StructField("status", StringType(), True),
            StructField("compliance", StructType([
                StructField("aml_score", DoubleType(), True),
                StructField("risk_level", StringType(), True)
            ]), True),
            StructField("stream_metadata", StructType([
                StructField("producer_id", StringType(), True),
                StructField("generated_at", StringType(), True),
                StructField("sequence_id", IntegerType(), True)
            ]), True)
        ])
    
    def process_bronze_data(self, topic_name: str):
        """Process data for Bronze layer from specific Kafka topic"""
        try:
            schema = self.define_schema()
            
            # Read from Kafka stream
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", topic_name) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON and add metadata
            bronze_df = kafka_df \
                .select(
                    from_json(col("value").cast("string"), schema).alias("data"),
                    col("topic").alias("kafka_topic"),
                    col("partition").alias("kafka_partition"),
                    col("offset").alias("kafka_offset"),
                    col("timestamp").alias("kafka_timestamp"),
                    current_timestamp().alias("ingestion_timestamp")
                ) \
                .select(
                    col("data.*"),
                    col("kafka_topic"),
                    col("kafka_partition"),
                    col("kafka_offset"),
                    col("kafka_timestamp"),
                    col("ingestion_timestamp"),
                    # Add partitioning columns
                    date_format(col("data.timestamp"), "yyyy-MM-dd").alias("date_partition"),
                    hour(col("data.timestamp")).alias("hour_partition")
                )
            
            # Write to HDFS Bronze layer (Parquet format, partitioned)
            bronze_query = bronze_df \
                .writeStream \
                .format("parquet") \
                .option("path", f"{self.hdfs_base_path}/{topic_name}") \
                .option("checkpointLocation", f"{self.checkpoint_location}/{topic_name}") \
                .partitionBy("date_partition", "hour_partition") \
                .outputMode("append") \
                .trigger(processingTime="30 seconds") \
                .start()
            
            logger.info(f"✅ Bronze layer streaming started for topic: {topic_name}")
            return bronze_query
            
        except Exception as e:
            logger.error(f"❌ Failed to process bronze data for {topic_name}: {e}")
            return None
    
    def create_hdfs_directories(self):
        """Create HDFS directory structure"""
        try:
            # Create directories via Spark SQL
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze_layer LOCATION '{self.hdfs_base_path}'")
            logger.info("✅ HDFS directory structure created")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Directory creation warning: {e}")
            return True  # Continue even if directories exist
    
    def run_bronze_pipeline(self):
        """Run the complete Bronze layer pipeline"""
        logger.info("🥉 Starting Bronze Layer Pipeline")
        logger.info("="*60)
        
        if not self.create_spark_session():
            return False
        
        # Create HDFS structure
        self.create_hdfs_directories()
        
        # Process each Kafka topic
        topics = ["transactions-normal", "transactions-suspicious", "transactions-fraud-alerts"]
        streaming_queries = []
        
        for topic in topics:
            query = self.process_bronze_data(topic)
            if query:
                streaming_queries.append(query)
        
        if not streaming_queries:
            logger.error("❌ No streaming queries started")
            return False
        
        logger.info(f"🚀 Bronze layer pipeline started for {len(streaming_queries)} topics")
        logger.info("📊 Data will be stored in HDFS as Parquet files")
        logger.info("🔄 Processing every 30 seconds...")
        
        # Keep the pipeline running
        try:
            for query in streaming_queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("🛑 Pipeline stopped by user")
        finally:
            for query in streaming_queries:
                if query.isActive:
                    query.stop()
            self.spark.stop()
        
        return True

def main():
    """Main entry point"""
    processor = BronzeLayerProcessor()
    
    # Show pipeline information
    print("🥉 BRONZE LAYER - RAW DATA INGESTION")
    print("="*60)
    print("📥 Source: Kafka Topics")
    print("📤 Destination: HDFS (Parquet)")
    print("🏗️ Architecture: Medallion (Bronze Layer)")
    print("🔄 Processing: Real-time streaming")
    print("📊 Partitioning: Date + Hour")
    print("="*60)
    
    success = processor.run_bronze_pipeline()
    
    if success:
        print("✅ Bronze layer pipeline completed successfully")
    else:
        print("❌ Bronze layer pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    main()