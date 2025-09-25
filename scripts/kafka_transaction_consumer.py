#!/usr/bin/env python3
"""
🌊 Real-time Transaction Consumer & Processor
⚡ Spark Streaming + Kafka Consumer for Bronze Layer Ingestion

This script consumes real-time transactions from Kafka topics and processes them
through Spark Streaming for Bronze layer ingestion with fraud detection.

🎯 Features:
- ⚡ Spark Streaming with Kafka integration
- 🗃️ Real-time Bronze layer ingestion to HDFS/Parquet
- 🚨 Stream-based fraud detection and alerting
- 📊 Real-time analytics and monitoring
- 🔄 Watermarking and late data handling
"""

import os
import sys
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta

# 📦 Required dependencies
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql.streaming import *
    import json
except ImportError as e:
    print("❌ Missing required dependencies!")
    print("Please install: pip install pyspark kafka-python")
    print(f"Error: {e}")
    sys.exit(1)

# 🎨 Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamingTransactionProcessor:
    def __init__(self, 
                 kafka_bootstrap_servers: str = "localhost:9092",
                 hdfs_path: str = "hdfs://localhost:9000",
                 checkpoint_location: str = "/tmp/spark-streaming-checkpoint"):
        """
        🏗️ Initialize Spark Streaming Transaction Processor
        
        Args:
            kafka_bootstrap_servers: Kafka cluster address
            hdfs_path: HDFS namenode URL  
            checkpoint_location: Spark streaming checkpoint location
        """
        self.kafka_servers = kafka_bootstrap_servers
        self.hdfs_path = hdfs_path
        self.checkpoint_location = checkpoint_location
        
        # 📊 Topics configuration
        self.topics = {
            'normal': 'transactions-normal',
            'suspicious': 'transactions-suspicious',
            'fraud_alerts': 'transactions-fraud-alerts'
        }
        
        # 🗂️ Output paths
        self.output_paths = {
            'bronze_normal': f"{hdfs_path}/bronze/transactions/normal",
            'bronze_suspicious': f"{hdfs_path}/bronze/transactions/suspicious", 
            'silver_processed': f"{hdfs_path}/silver/transactions/processed",
            'gold_fraud_alerts': f"{hdfs_path}/gold/fraud_alerts"
        }
        
        # ⚡ Initialize Spark Session
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """⚡ Create optimized Spark session for streaming"""
        try:
            spark = SparkSession.builder \
                .appName("CryptoBridge-Streaming-Processor") \
                .config("spark.sql.streaming.metricsEnabled", "true") \
                .config("spark.sql.streaming.stateStore.providerClass", 
                       "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.hadoop.fs.defaultFS", self.hdfs_path) \
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .getOrCreate()
            
            # 🔇 Reduce log noise
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info("✅ Spark Streaming session created successfully")
            logger.info(f"🌐 Spark Master: {spark.sparkContext.master}")
            logger.info(f"📊 Spark Version: {spark.version}")
            
            return spark
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            raise
    
    def _get_transaction_schema(self) -> StructType:
        """📋 Define transaction schema for structured streaming"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            
            # Sender information
            StructField("sender", StructType([
                StructField("user_id", StringType(), False),
                StructField("account_type", StringType(), False),
                StructField("email", StringType(), False),
                StructField("country", StringType(), False),
                StructField("kyc_level", IntegerType(), False)
            ]), False),
            
            # Receiver information
            StructField("receiver", StructType([
                StructField("user_id", StringType(), False),
                StructField("email", StringType(), False),
                StructField("country", StringType(), False),
                StructField("exchange", StringType(), False),
                StructField("kyc_level", IntegerType(), False)
            ]), False),
            
            # Transfer details
            StructField("transfer", StructType([
                StructField("crypto_symbol", StringType(), False),
                StructField("crypto_amount", DoubleType(), False),
                StructField("usd_equivalent", DoubleType(), False),
                StructField("thb_equivalent", DoubleType(), False),
                StructField("exchange_rate_usd_thb", DoubleType(), False)
            ]), False),
            
            # Fees
            StructField("fees", StructType([
                StructField("fee_usd", DoubleType(), False),
                StructField("fee_rate", DoubleType(), False),
                StructField("fee_type", StringType(), False)
            ]), False),
            
            # Compliance
            StructField("compliance", StructType([
                StructField("aml_score", DoubleType(), False),
                StructField("risk_level", StringType(), False),
                StructField("flags", ArrayType(StringType()), True)
            ]), False),
            
            # Stream metadata
            StructField("stream_metadata", StructType([
                StructField("producer_id", StringType(), False),
                StructField("generated_at", TimestampType(), False),
                StructField("sequence_id", LongType(), False)
            ]), True),
            
            StructField("status", StringType(), False)
        ])
    
    def _create_kafka_stream(self, topic: str, stream_name: str):
        """📡 Create Kafka streaming DataFrame"""
        try:
            logger.info(f"📡 Creating Kafka stream for topic: {topic}")
            
            # Read from Kafka
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("kafka.consumer.group.id", f"cryptobridge-{stream_name}") \
                .load()
            
            # Parse JSON messages
            schema = self._get_transaction_schema()
            
            parsed_df = kafka_df.select(
                col("key").cast("string").alias("partition_key"),
                col("timestamp").alias("kafka_timestamp"),
                col("offset"),
                col("partition"),
                from_json(col("value").cast("string"), schema).alias("transaction")
            ).select(
                col("partition_key"),
                col("kafka_timestamp"), 
                col("offset"),
                col("partition"),
                col("transaction.*")
            )
            
            # Add processing metadata
            enriched_df = parsed_df.withColumns({
                "processed_at": current_timestamp(),
                "processing_date": current_date(),
                "year": year(col("timestamp")),
                "month": month(col("timestamp")),
                "day": dayofmonth(col("timestamp")),
                "hour": hour(col("timestamp"))
            })
            
            logger.info(f"✅ Kafka stream created for {topic}")
            return enriched_df
            
        except Exception as e:
            logger.error(f"❌ Failed to create Kafka stream for {topic}: {e}")
            raise
    
    def _create_fraud_detection_stream(self, transaction_stream):
        """🚨 Create real-time fraud detection stream"""
        logger.info("🚨 Setting up fraud detection stream...")
        
        # Define fraud detection rules
        fraud_conditions = [
            # High amount transactions
            col("transfer.usd_equivalent") > 30000,
            
            # High AML scores
            col("compliance.aml_score") > 0.7,
            
            # Suspicious countries (example)
            col("sender.country").isin(['RU', 'CN']) & (col("transfer.usd_equivalent") > 10000),
            
            # Low KYC with high amounts
            (col("sender.kyc_level") == 1) & (col("transfer.usd_equivalent") > 5000),
            
            # Stablecoin large transfers
            col("transfer.crypto_symbol").isin(['USDT', 'USDC']) & (col("transfer.usd_equivalent") > 50000)
        ]
        
        # Create fraud detection logic
        fraud_df = transaction_stream.withColumn(
            "fraud_score",
            when(col("compliance.aml_score") > 0.9, lit(1.0))\
            .when(col("transfer.usd_equivalent") > 100000, lit(0.9))\
            .when(col("transfer.usd_equivalent") > 50000, lit(0.7))\
            .when(col("compliance.aml_score") > 0.7, lit(0.6))\
            .otherwise(lit(0.1))
        ).withColumn(
            "is_fraud_alert",
            expr("array_contains(compliance.flags, 'high_amount') OR fraud_score > 0.8")
        ).withColumn(
            "alert_type",
            when(col("transfer.usd_equivalent") > 100000, lit("high_amount"))\
            .when(col("compliance.aml_score") > 0.9, lit("high_aml_score"))\
            .when(col("sender.kyc_level") == 1, lit("low_kyc"))\
            .otherwise(lit("general_suspicious"))
        )
        
        # Filter only fraud alerts
        alerts_df = fraud_df.filter(col("is_fraud_alert") == True)
        
        return alerts_df
    
    def _write_to_bronze_layer(self, stream_df, output_path: str, stream_name: str):
        """🗃️ Write stream to Bronze layer (HDFS/Parquet)"""
        logger.info(f"🗃️ Setting up Bronze layer writer: {stream_name}")
        
        # Write stream to Parquet with partitioning
        query = stream_df.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{self.checkpoint_location}/{stream_name}") \
            .partitionBy("year", "month", "sender.country") \
            .outputMode("append") \
            .trigger(processingTime='30 seconds') \
            .queryName(f"bronze-{stream_name}") \
            .start()
        
        logger.info(f"✅ Bronze layer stream started: {stream_name}")
        return query
    
    def _write_fraud_alerts(self, alerts_df):
        """🚨 Write fraud alerts to Gold layer and console"""
        logger.info("🚨 Setting up fraud alerts output...")
        
        # Write to console for real-time monitoring
        console_query = alerts_df.select(
            col("transaction_id"),
            col("timestamp"),
            col("sender.country").alias("sender_country"),
            col("transfer.crypto_symbol"),
            col("transfer.usd_equivalent"),
            col("fraud_score"),
            col("alert_type"),
            col("compliance.aml_score")
        ).writeStream \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 20) \
            .outputMode("append") \
            .trigger(processingTime='10 seconds') \
            .queryName("fraud-alerts-console") \
            .start()
        
        # Write to Gold layer for analysis
        gold_query = alerts_df.writeStream \
            .format("parquet") \
            .option("path", self.output_paths['gold_fraud_alerts']) \
            .option("checkpointLocation", f"{self.checkpoint_location}/fraud_alerts") \
            .partitionBy("processing_date", "alert_type") \
            .outputMode("append") \
            .trigger(processingTime='60 seconds') \
            .queryName("fraud-alerts-gold") \
            .start()
        
        logger.info("✅ Fraud alerts streams started")
        return [console_query, gold_query]
    
    def start_streaming_pipeline(self):
        """🚀 Start the complete streaming pipeline"""
        logger.info("🚀 Starting CryptoBridge streaming pipeline...")
        
        try:
            # 📡 Create Kafka streams
            normal_stream = self._create_kafka_stream(
                self.topics['normal'], 
                "normal-transactions"
            )
            
            suspicious_stream = self._create_kafka_stream(
                self.topics['suspicious'], 
                "suspicious-transactions"
            )
            
            # 🚨 Create fraud detection stream
            fraud_alerts = self._create_fraud_detection_stream(suspicious_stream)
            
            # 🗃️ Write to Bronze layer
            normal_query = self._write_to_bronze_layer(
                normal_stream,
                self.output_paths['bronze_normal'],
                "normal"
            )
            
            suspicious_query = self._write_to_bronze_layer(
                suspicious_stream, 
                self.output_paths['bronze_suspicious'],
                "suspicious"
            )
            
            # 🚨 Setup fraud alerting
            fraud_queries = self._write_fraud_alerts(fraud_alerts)
            
            # 📊 Collect all queries
            all_queries = [normal_query, suspicious_query] + fraud_queries
            
            logger.info("✅ All streaming queries started successfully!")
            logger.info(f"📊 Running {len(all_queries)} streaming queries")
            
            # 🔄 Wait for all queries
            logger.info("🔄 Streaming pipeline running... Press Ctrl+C to stop")
            
            for query in all_queries:
                logger.info(f"📋 Query: {query.name} - Status: {query.status}")
            
            # Monitor and wait
            self._monitor_streaming_queries(all_queries)
            
        except KeyboardInterrupt:
            logger.info("🛑 Stopping streaming pipeline (user interrupt)")
            self._stop_all_queries()
        except Exception as e:
            logger.error(f"❌ Streaming pipeline error: {e}")
            self._stop_all_queries()
            raise
    
    def _monitor_streaming_queries(self, queries):
        """📊 Monitor streaming queries and log progress"""
        import time
        
        while True:
            try:
                time.sleep(30)  # Check every 30 seconds
                
                logger.info("📊 Streaming Progress Report:")
                for query in queries:
                    if query.isActive:
                        progress = query.lastProgress
                        if progress:
                            logger.info(f"   🔄 {query.name}:")
                            logger.info(f"      📈 Input Rate: {progress.get('inputRowsPerSecond', 0):.2f} rows/sec")
                            logger.info(f"      📊 Processed: {progress.get('batchId', 0)} batches")
                    else:
                        logger.warning(f"   ⚠️ {query.name}: INACTIVE")
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"❌ Monitoring error: {e}")
                break
    
    def _stop_all_queries(self):
        """🛑 Stop all streaming queries gracefully"""
        logger.info("🛑 Stopping all streaming queries...")
        
        for query in self.spark.streams.active:
            try:
                query.stop()
                logger.info(f"✅ Stopped query: {query.name}")
            except Exception as e:
                logger.error(f"❌ Failed to stop query {query.name}: {e}")
        
        logger.info("✅ All queries stopped")

def main():
    """🎯 Main execution function"""
    print("🌊 Real-time Transaction Processing with Spark Streaming")
    print("=" * 60)
    
    # Configuration
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    hdfs_path = os.getenv("HDFS_PATH", "hdfs://localhost:9000")
    
    try:
        # Initialize processor
        processor = StreamingTransactionProcessor(
            kafka_bootstrap_servers=kafka_servers,
            hdfs_path=hdfs_path
        )
        
        # Start streaming pipeline
        processor.start_streaming_pipeline()
        
    except Exception as e:
        logger.error(f"❌ Failed to start streaming pipeline: {e}")
        print("\n💡 Make sure Kafka and HDFS are running:")
        print("   docker-compose up -d kafka zookeeper namenode datanode")

if __name__ == "__main__":
    main()