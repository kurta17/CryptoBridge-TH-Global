#!/usr/bin/env python3
"""
🥈 Silver Layer - Clean & Transformed Data Pipeline
Bronze → Silver - Medallion Architecture

This pipeline reads raw data from Bronze layer, applies data quality rules,
standardization, and enrichment to create clean, analysis-ready datasets.

🎯 Silver Layer Features:
- 🧹 Data quality validation and cleansing
- 🔄 Schema standardization and normalization
- 🌍 Currency conversion and standardization
- 🚨 Enhanced fraud detection features
- 📊 Business rule applications
- 🏷️ Data classification and tagging
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql.window import Window
    import json
except ImportError as e:
    print("❌ Missing PySpark dependencies!")
    print("Please install: pip install pyspark")
    print(f"Error: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverLayerProcessor:
    def __init__(self):
        """Initialize Silver Layer Processor"""
        self.spark = None
        self.bronze_path = "hdfs://namenode:9000/cryptobridge/bronze"
        self.silver_path = "hdfs://namenode:9000/cryptobridge/silver"
        self.checkpoint_location = "hdfs://localhost:9001/checkpoints/silver"
        
    def create_spark_session(self):
        """Create Spark session with optimized configurations"""
        try:
            self.spark = SparkSession.builder \
                .appName("CryptoBridge-Silver-Layer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.sql.warehouse.dir", "hdfs://localhost:9001/warehouse") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9001") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark session created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            return False
    
    def define_data_quality_rules(self):
        """Define data quality validation rules"""
        return {
            "required_fields": [
                "transaction_id", "timestamp", "sender", "receiver", 
                "transfer", "status", "compliance"
            ],
            "amount_validation": {
                "min_amount": 0.01,
                "max_amount": 1000000.00
            },
            "country_codes": ["TH", "PH", "SG", "MY", "VN", "ID", "US", "GB", "JP"],
            "crypto_symbols": ["BTC", "ETH", "XRP", "ADA", "DOT", "AVAX", "MATIC"],
            "status_values": ["pending", "completed", "failed", "cancelled"],
            "risk_levels": ["low", "medium", "high"]
        }
    
    def create_cleaning_functions(self):
        """Create UDFs for data cleaning and standardization"""
        
        # Currency standardization UDF
        @udf(returnType=StringType())
        def standardize_currency(symbol):
            if symbol is None:
                return "UNKNOWN"
            return symbol.upper().strip()
        
        # Country code standardization UDF
        @udf(returnType=StringType())
        def standardize_country(country):
            if country is None:
                return "UNKNOWN"
            country_mapping = {
                "THAILAND": "TH", "PHILIPPINES": "PH", "SINGAPORE": "SG",
                "MALAYSIA": "MY", "VIETNAM": "VN", "INDONESIA": "ID",
                "UNITED STATES": "US", "GREAT BRITAIN": "GB", "JAPAN": "JP"
            }
            clean_country = country.upper().strip()
            return country_mapping.get(clean_country, clean_country)
        
        # Enhanced fraud score calculation UDF
        @udf(returnType=DoubleType())
        def calculate_enhanced_fraud_score(aml_score, amount, from_country, to_country, kyc_sender, kyc_receiver):
            base_score = float(aml_score) if aml_score else 0.0
            
            # Amount-based risk
            if amount and amount > 50000:
                base_score += 0.1
            elif amount and amount > 100000:
                base_score += 0.2
            
            # Cross-border risk
            if from_country != to_country:
                base_score += 0.05
            
            # KYC level risk
            if kyc_sender and kyc_sender < 2:
                base_score += 0.1
            if kyc_receiver and kyc_receiver < 2:
                base_score += 0.1
            
            return min(base_score, 1.0)  # Cap at 1.0
        
        return {
            "standardize_currency": standardize_currency,
            "standardize_country": standardize_country,
            "calculate_enhanced_fraud_score": calculate_enhanced_fraud_score
        }
    
    def apply_data_quality_checks(self, df):
        """Apply comprehensive data quality checks"""
        quality_rules = self.define_data_quality_rules()
        
        # Add data quality flags
        quality_df = df \
            .withColumn("dq_transaction_id_valid", 
                       when(col("transaction_id").isNotNull() & (length(col("transaction_id")) > 0), True).otherwise(False)) \
            .withColumn("dq_amount_valid",
                       when((col("transfer.usd_equivalent") >= quality_rules["amount_validation"]["min_amount"]) &
                            (col("transfer.usd_equivalent") <= quality_rules["amount_validation"]["max_amount"]), True).otherwise(False)) \
            .withColumn("dq_country_valid",
                       when(col("sender.country").isin(quality_rules["country_codes"]) &
                            col("receiver.country").isin(quality_rules["country_codes"]), True).otherwise(False)) \
            .withColumn("dq_currency_valid",
                       when(col("transfer.crypto_symbol").isin(quality_rules["crypto_symbols"]), True).otherwise(False)) \
            .withColumn("dq_status_valid",
                       when(col("status").isin(quality_rules["status_values"]), True).otherwise(False)) \
            .withColumn("dq_risk_level_valid",
                       when(col("compliance.risk_level").isin(quality_rules["risk_levels"]), True).otherwise(False))
        
        # Calculate overall data quality score
        quality_df = quality_df \
            .withColumn("data_quality_score",
                       (col("dq_transaction_id_valid").cast("int") +
                        col("dq_amount_valid").cast("int") +
                        col("dq_country_valid").cast("int") +
                        col("dq_currency_valid").cast("int") +
                        col("dq_status_valid").cast("int") +
                        col("dq_risk_level_valid").cast("int")) / 6.0) \
            .withColumn("is_high_quality", col("data_quality_score") >= 0.8)
        
        return quality_df
    
    def transform_to_silver(self, bronze_df):
        """Transform Bronze data to Silver with cleaning and enrichment"""
        cleaning_functions = self.create_cleaning_functions()
        
        # Flatten and clean the data structure
        silver_df = bronze_df \
            .select(
                # Core transaction fields
                col("transaction_id"),
                col("timestamp").alias("transaction_timestamp"),
                col("status"),
                
                # Sender information (flattened and cleaned)
                col("sender.user_id").alias("sender_user_id"),
                col("sender.account_type").alias("sender_account_type"),
                col("sender.email").alias("sender_email"),
                cleaning_functions["standardize_country"](col("sender.country")).alias("sender_country"),
                col("sender.kyc_level").alias("sender_kyc_level"),
                col("sender.preferred_crypto").alias("sender_preferred_crypto"),
                
                # Receiver information (flattened and cleaned)
                col("receiver.user_id").alias("receiver_user_id"),
                col("receiver.email").alias("receiver_email"),
                cleaning_functions["standardize_country"](col("receiver.country")).alias("receiver_country"),
                col("receiver.exchange").alias("receiver_exchange"),
                col("receiver.kyc_level").alias("receiver_kyc_level"),
                
                # Transfer information (standardized)
                cleaning_functions["standardize_currency"](col("transfer.crypto_symbol")).alias("crypto_currency"),
                col("transfer.crypto_amount"),
                col("transfer.usd_equivalent").alias("usd_amount"),
                col("transfer.thb_equivalent").alias("thb_amount"),
                col("transfer.exchange_rate_usd_thb"),
                
                # Fee information
                col("fees.fee_usd"),
                col("fees.fee_rate"),
                col("fees.fee_type"),
                
                # Compliance and fraud detection
                col("compliance.aml_score").alias("original_aml_score"),
                col("compliance.risk_level").alias("original_risk_level"),
                
                # Metadata
                col("kafka_topic"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("kafka_timestamp"),
                col("ingestion_timestamp"),
                col("date_partition"),
                col("hour_partition")
            )
        
        # Add enhanced fraud scoring
        silver_df = silver_df \
            .withColumn("enhanced_fraud_score",
                       cleaning_functions["calculate_enhanced_fraud_score"](
                           col("original_aml_score"),
                           col("usd_amount"),
                           col("sender_country"),
                           col("receiver_country"),
                           col("sender_kyc_level"),
                           col("receiver_kyc_level")
                       ))
        
        # Add business intelligence fields
        silver_df = silver_df \
            .withColumn("is_cross_border", col("sender_country") != col("receiver_country")) \
            .withColumn("transaction_category",
                       when(col("usd_amount") <= 1000, "micro")
                       .when(col("usd_amount") <= 10000, "small")
                       .when(col("usd_amount") <= 50000, "medium")
                       .otherwise("large")) \
            .withColumn("risk_category",
                       when(col("enhanced_fraud_score") <= 0.3, "low_risk")
                       .when(col("enhanced_fraud_score") <= 0.6, "medium_risk")
                       .otherwise("high_risk")) \
            .withColumn("kyc_compliance_level",
                       when((col("sender_kyc_level") >= 2) & (col("receiver_kyc_level") >= 2), "compliant")
                       .when((col("sender_kyc_level") >= 1) & (col("receiver_kyc_level") >= 1), "partial")
                       .otherwise("non_compliant"))
        
        # Add processing timestamp
        silver_df = silver_df \
            .withColumn("silver_processing_timestamp", current_timestamp()) \
            .withColumn("processing_date", current_date())
        
        return silver_df
    
    def process_silver_layer(self, topic_name: str):
        """Process Bronze data to Silver layer"""
        try:
            # Read from Bronze layer
            bronze_df = self.spark \
                .readStream \
                .format("parquet") \
                .option("path", f"{self.bronze_path}/{topic_name}") \
                .load()
            
            # Apply data quality checks
            quality_df = self.apply_data_quality_checks(bronze_df)
            
            # Transform to Silver
            silver_df = self.transform_to_silver(quality_df)
            
            # Write to Silver layer
            silver_query = silver_df \
                .writeStream \
                .format("parquet") \
                .option("path", f"{self.silver_path}/{topic_name}") \
                .option("checkpointLocation", f"{self.checkpoint_location}/{topic_name}") \
                .partitionBy("processing_date", "sender_country", "receiver_country") \
                .outputMode("append") \
                .trigger(processingTime="60 seconds") \
                .start()
            
            logger.info(f"✅ Silver layer streaming started for topic: {topic_name}")
            return silver_query
            
        except Exception as e:
            logger.error(f"❌ Failed to process silver data for {topic_name}: {e}")
            return None
    
    def run_silver_pipeline(self):
        """Run the complete Silver layer pipeline"""
        logger.info("🥈 Starting Silver Layer Pipeline")
        logger.info("="*60)
        
        if not self.create_spark_session():
            return False
        
        # Process each topic from Bronze to Silver
        topics = ["transactions-normal", "transactions-suspicious", "transactions-fraud-alerts"]
        streaming_queries = []
        
        for topic in topics:
            query = self.process_silver_layer(topic)
            if query:
                streaming_queries.append(query)
        
        if not streaming_queries:
            logger.error("❌ No streaming queries started")
            return False
        
        logger.info(f"🚀 Silver layer pipeline started for {len(streaming_queries)} topics")
        logger.info("🧹 Applied data quality checks and cleaning")
        logger.info("🔄 Processing every 60 seconds...")
        
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
    processor = SilverLayerProcessor()
    
    # Show pipeline information
    print("🥈 SILVER LAYER - CLEAN & TRANSFORMED DATA")
    print("="*60)
    print("📥 Source: Bronze Layer (HDFS Parquet)")
    print("📤 Destination: Silver Layer (HDFS Parquet)")
    print("🏗️ Architecture: Medallion (Silver Layer)")
    print("🧹 Processing: Data Quality + Cleaning + Enrichment")
    print("📊 Partitioning: Date + Country Flows")
    print("="*60)
    
    success = processor.run_silver_pipeline()
    
    if success:
        print("✅ Silver layer pipeline completed successfully")
    else:
        print("❌ Silver layer pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    main()