#!/usr/bin/env python3
"""
🥇 Gold Layer - Business Analytics & Aggregates Pipeline
Silver → Gold - Medallion Architecture

This pipeline creates business-ready analytics, KPIs, and aggregated datasets
from the clean Silver layer data for executive dashboards and reporting.

🎯 Gold Layer Features:
- 📊 Business KPIs and metrics
- 🏦 Daily/Hourly transaction summaries
- 🌍 Country flow analytics
- 💰 Revenue and fee analysis
- 🚨 Fraud detection summaries
- 📈 Trend analysis and forecasting
- 🎯 Executive dashboards data
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

class GoldLayerProcessor:
    def __init__(self):
        """Initialize Gold Layer Processor"""
        self.spark = None
        self.silver_path = "hdfs://namenode:9000/cryptobridge/silver"
        self.gold_path = "hdfs://namenode:9000/cryptobridge/gold"
        self.checkpoint_location = "hdfs://localhost:9001/checkpoints/gold"
        
    def create_spark_session(self):
        """Create Spark session with optimized configurations"""
        try:
            self.spark = SparkSession.builder \
                .appName("CryptoBridge-Gold-Layer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.sql.warehouse.dir", "hdfs://localhost:9001/warehouse") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9001") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark session created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            return False
    
    def create_transaction_summary_aggregates(self, silver_df):
        """Create transaction summary aggregates"""
        
        # Daily transaction summaries
        daily_summary = silver_df \
            .groupBy("processing_date", "sender_country", "receiver_country", "crypto_currency") \
            .agg(
                count("transaction_id").alias("transaction_count"),
                sum("usd_amount").alias("total_usd_volume"),
                avg("usd_amount").alias("avg_transaction_amount"),
                min("usd_amount").alias("min_transaction_amount"),
                max("usd_amount").alias("max_transaction_amount"),
                sum("fee_usd").alias("total_fees_collected"),
                avg("enhanced_fraud_score").alias("avg_fraud_score"),
                sum(when(col("risk_category") == "high_risk", 1).otherwise(0)).alias("high_risk_count"),
                sum(when(col("is_cross_border") == True, 1).otherwise(0)).alias("cross_border_count"),
                sum(when(col("kyc_compliance_level") == "compliant", 1).otherwise(0)).alias("compliant_count"),
                countDistinct("sender_user_id").alias("unique_senders"),
                countDistinct("receiver_user_id").alias("unique_receivers")
            ) \
            .withColumn("avg_fraud_score", round(col("avg_fraud_score"), 4)) \
            .withColumn("cross_border_percentage", round((col("cross_border_count") / col("transaction_count")) * 100, 2)) \
            .withColumn("high_risk_percentage", round((col("high_risk_count") / col("transaction_count")) * 100, 2)) \
            .withColumn("compliance_percentage", round((col("compliant_count") / col("transaction_count")) * 100, 2)) \
            .withColumn("aggregation_level", lit("daily")) \
            .withColumn("created_timestamp", current_timestamp())
        
        return daily_summary
    
    def create_country_flow_analytics(self, silver_df):
        """Create country-to-country flow analytics"""
        
        country_flows = silver_df \
            .groupBy("processing_date", "sender_country", "receiver_country") \
            .agg(
                count("transaction_id").alias("flow_transaction_count"),
                sum("usd_amount").alias("flow_total_volume"),
                avg("usd_amount").alias("flow_avg_amount"),
                sum("fee_usd").alias("flow_total_fees"),
                avg("enhanced_fraud_score").alias("flow_avg_fraud_score"),
                countDistinct("crypto_currency").alias("currencies_used"),
                sum(when(col("transaction_category") == "large", 1).otherwise(0)).alias("large_transactions"),
                max("usd_amount").alias("largest_transaction")
            ) \
            .withColumn("flow_direction", concat(col("sender_country"), lit("_to_"), col("receiver_country"))) \
            .withColumn("is_domestic", col("sender_country") == col("receiver_country")) \
            .withColumn("flow_risk_level", 
                       when(col("flow_avg_fraud_score") <= 0.3, "low")
                       .when(col("flow_avg_fraud_score") <= 0.6, "medium")
                       .otherwise("high")) \
            .withColumn("created_timestamp", current_timestamp())
        
        return country_flows
    
    def create_fraud_analytics(self, silver_df):
        """Create comprehensive fraud analytics"""
        
        fraud_analytics = silver_df \
            .groupBy("processing_date", "risk_category", "sender_country", "receiver_country") \
            .agg(
                count("transaction_id").alias("risk_transaction_count"),
                sum("usd_amount").alias("risk_total_volume"),
                avg("enhanced_fraud_score").alias("avg_enhanced_fraud_score"),
                avg("original_aml_score").alias("avg_original_aml_score"),
                sum(when(col("kyc_compliance_level") == "non_compliant", 1).otherwise(0)).alias("non_compliant_count"),
                sum(when(col("transaction_category") == "large", 1).otherwise(0)).alias("large_amount_count"),
                countDistinct("sender_user_id").alias("unique_risk_senders"),
                countDistinct("receiver_user_id").alias("unique_risk_receivers"),
                max("enhanced_fraud_score").alias("max_fraud_score"),
                collect_list("transaction_id").alias("sample_transaction_ids")
            ) \
            .withColumn("fraud_improvement", col("avg_enhanced_fraud_score") - col("avg_original_aml_score")) \
            .withColumn("risk_concentration", col("risk_transaction_count") / sum("risk_transaction_count").over(Window.partitionBy("processing_date"))) \
            .withColumn("created_timestamp", current_timestamp())
        
        return fraud_analytics
    
    def create_business_kpis(self, silver_df):
        """Create executive-level business KPIs"""
        
        # Calculate KPIs with window functions for trends
        daily_window = Window.partitionBy().orderBy("processing_date")
        
        business_kpis = silver_df \
            .groupBy("processing_date") \
            .agg(
                # Volume metrics
                count("transaction_id").alias("daily_transaction_volume"),
                sum("usd_amount").alias("daily_usd_volume"),
                avg("usd_amount").alias("daily_avg_transaction_size"),
                
                # Revenue metrics
                sum("fee_usd").alias("daily_revenue"),
                avg("fee_rate").alias("avg_fee_rate"),
                
                # User metrics
                countDistinct("sender_user_id").alias("daily_active_senders"),
                countDistinct("receiver_user_id").alias("daily_active_receivers"),
                
                # Risk metrics
                avg("enhanced_fraud_score").alias("daily_avg_fraud_score"),
                sum(when(col("risk_category") == "high_risk", 1).otherwise(0)).alias("daily_high_risk_transactions"),
                
                # Compliance metrics
                sum(when(col("kyc_compliance_level") == "compliant", 1).otherwise(0)).alias("daily_compliant_transactions"),
                sum(when(col("is_cross_border") == True, 1).otherwise(0)).alias("daily_cross_border_transactions"),
                
                # Currency distribution
                countDistinct("crypto_currency").alias("currencies_traded"),
                
                # Geographic metrics
                countDistinct("sender_country").alias("countries_sending"),
                countDistinct("receiver_country").alias("countries_receiving")
            ) \
            .withColumn("cross_border_rate", round((col("daily_cross_border_transactions") /col("daily_transaction_volume")) * 100, 2)) \
            .withColumn("compliance_rate", round((col("daily_compliant_transactions") / col("daily_transaction_volume")) * 100, 2)) \
            .withColumn("high_risk_rate", round((col("daily_high_risk_transactions") / col("daily_transaction_volume")) * 100, 2)) \
            .withColumn("revenue_per_transaction", round(col("daily_revenue") / col("daily_transaction_volume"), 4)) \
            .withColumn("avg_transaction_size_trend", 
                       col("daily_avg_transaction_size") - lag("daily_avg_transaction_size", 1).over(daily_window)) \
            .withColumn("volume_growth_rate",
                       ((col("daily_transaction_volume") - lag("daily_transaction_volume", 1).over(daily_window)) / 
                        lag("daily_transaction_volume", 1).over(daily_window)) * 100) \
            .withColumn("created_timestamp", current_timestamp())
        
        return business_kpis
    
    def create_hourly_trends(self, silver_df):
        """Create hourly trend analysis"""
        
        hourly_trends = silver_df \
            .withColumn("transaction_hour", hour(col("transaction_timestamp"))) \
            .groupBy("processing_date", "transaction_hour") \
            .agg(
                count("transaction_id").alias("hourly_count"),
                sum("usd_amount").alias("hourly_volume"),
                avg("enhanced_fraud_score").alias("hourly_avg_fraud_score"),
                countDistinct("sender_country").alias("hourly_countries"),
                sum("fee_usd").alias("hourly_revenue")
            ) \
            .withColumn("hour_classification",
                       when(col("transaction_hour").between(6, 11), "morning")
                       .when(col("transaction_hour").between(12, 17), "afternoon")
                       .when(col("transaction_hour").between(18, 23), "evening")
                       .otherwise("night")) \
            .withColumn("created_timestamp", current_timestamp())
        
        return hourly_trends
    
    def process_gold_aggregates(self):
        """Process all Gold layer aggregates"""
        try:
            # Read from Silver layer (all topics combined)
            silver_df = self.spark \
                .readStream \
                .format("parquet") \
                .option("path", f"{self.silver_path}/*") \
                .load()
            
            # Create different types of aggregates
            aggregates = {
                "transaction_summary": self.create_transaction_summary_aggregates(silver_df),
                "country_flows": self.create_country_flow_analytics(silver_df),
                "fraud_analytics": self.create_fraud_analytics(silver_df),
                "business_kpis": self.create_business_kpis(silver_df),
                "hourly_trends": self.create_hourly_trends(silver_df)
            }
            
            streaming_queries = []
            
            # Start streaming queries for each aggregate
            for aggregate_name, aggregate_df in aggregates.items():
                query = aggregate_df \
                    .writeStream \
                    .format("parquet") \
                    .option("path", f"{self.gold_path}/{aggregate_name}") \
                    .option("checkpointLocation", f"{self.checkpoint_location}/{aggregate_name}") \
                    .outputMode("complete" if aggregate_name == "business_kpis" else "append") \
                    .trigger(processingTime="120 seconds") \
                    .start()
                
                streaming_queries.append(query)
                logger.info(f"✅ Gold layer aggregate started: {aggregate_name}")
            
            return streaming_queries
            
        except Exception as e:
            logger.error(f"❌ Failed to process gold aggregates: {e}")
            return []
    
    def run_gold_pipeline(self):
        """Run the complete Gold layer pipeline"""
        logger.info("🥇 Starting Gold Layer Pipeline")
        logger.info("="*60)
        
        if not self.create_spark_session():
            return False
        
        # Process Gold layer aggregates
        streaming_queries = self.process_gold_aggregates()
        
        if not streaming_queries:
            logger.error("❌ No streaming queries started")
            return False
        
        logger.info(f"🚀 Gold layer pipeline started with {len(streaming_queries)} aggregates")
        logger.info("📊 Creating business KPIs and analytics")
        logger.info("🔄 Processing every 2 minutes...")
        
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
    processor = GoldLayerProcessor()
    
    # Show pipeline information
    print("🥇 GOLD LAYER - BUSINESS ANALYTICS & AGGREGATES")
    print("="*60)
    print("📥 Source: Silver Layer (HDFS Parquet)")
    print("📤 Destination: Gold Layer (HDFS Parquet)")
    print("🏗️ Architecture: Medallion (Gold Layer)")
    print("📊 Processing: Business KPIs + Analytics + Trends")
    print("🎯 Output: Executive Dashboards + Reports")
    print("="*60)
    print()
    print("📈 Gold Layer Outputs:")
    print("  • Transaction Summary Aggregates")
    print("  • Country Flow Analytics")
    print("  • Fraud Detection Analytics")
    print("  • Business KPIs & Metrics")
    print("  • Hourly Trend Analysis")
    print("="*60)
    
    success = processor.run_gold_pipeline()
    
    if success:
        print("✅ Gold layer pipeline completed successfully")
    else:
        print("❌ Gold layer pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    main()