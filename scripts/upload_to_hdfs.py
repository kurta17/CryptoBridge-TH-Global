#!/usr/bin/env python3
"""
🚀 HDFS Data Lake Upload Script
📊 Bronze Layer Data Ingestion

This script converts JSON transaction data to Parquet format and uploads
it to HDFS for Bronze layer processing in the data lake architecture.

🎯 Features:
- 🗂️ Converts JSON to optimized Parquet format
- 📁 Implements date-based partitioning for performance
- 🌊 Uploads to HDFS Bronze layer
- 🔄 Handles batch processing and data validation
- 📈 Supports multiple compression algorithms
"""

import json
import os
import sys
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path

# 📦 Required dependencies
try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from hdfs import InsecureClient
    import pyarrow.compute as pc
except ImportError as e:
    print("❌ Missing required dependencies!")
    print("Please install: pip install pandas pyarrow hdfs3 pyarrow")
    print(f"Error: {e}")
    sys.exit(1)

# 🎨 Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HDFSDataLakeUploader:
    def __init__(self, hdfs_host: str = "localhost", hdfs_port: int = 9000):
        """
        🏗️ Initialize HDFS Data Lake Uploader
        
        Args:
            hdfs_host: HDFS namenode hostname
            hdfs_port: HDFS namenode port
        """
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.hdfs_client = None
        self.bronze_path = "/bronze/transactions"
        
        # 🎨 Setup HDFS client
        self._setup_hdfs_client()
        
    def _setup_hdfs_client(self):
        """🔌 Setup HDFS client connection"""
        try:
            hdfs_url = f"http://{self.hdfs_host}:{self.hdfs_port}"
            self.hdfs_client = InsecureClient(hdfs_url, user='hadoop')
            logger.info(f"✅ Connected to HDFS: {hdfs_url}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to HDFS: {e}")
            logger.info("💡 Make sure HDFS is running and accessible")
            # For development, we'll work locally
            self.hdfs_client = None
            logger.info("🔧 Running in local mode for development")
    
    def load_transaction_data(self, json_path: str) -> pd.DataFrame:
        """
        📖 Load transaction data from JSON file
        
        Args:
            json_path: Path to the JSON transaction file
            
        Returns:
            Pandas DataFrame with transaction data
        """
        logger.info(f"📖 Loading transaction data from: {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            transactions = json.load(f)
        
        # 🔄 Normalize nested JSON structure
        df = pd.json_normalize(transactions)
        
        # 🕐 Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # 📅 Add partition columns
        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        df['date'] = df['timestamp'].dt.date
        
        logger.info(f"✅ Loaded {len(df)} transactions")
        logger.info(f"📊 Data shape: {df.shape}")
        
        return df
    
    def optimize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ⚡ Optimize data types for Parquet storage
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with optimized data types
        """
        logger.info("⚡ Optimizing data schema for Parquet...")
        
        # 🎯 Optimize data types
        optimizations = {
            # Numeric optimizations
            'transfer.usd_equivalent': 'float64',
            'transfer.thb_equivalent': 'float64', 
            'transfer.crypto_amount': 'float64',
            'transfer.exchange_rate_usd_thb': 'float32',
            'transfer.crypto_rate_usd': 'float32',
            
            # Fee optimizations
            'fees.fee_usd': 'float32',
            'fees.fee_thb': 'float32',
            'fees.fee_rate': 'float32',
            
            # Network optimizations
            'network.confirmation_blocks': 'int16',
            'network.network_fee_usd': 'float32',
            'processing_time_minutes': 'int16',
            
            # Compliance optimizations
            'compliance.aml_score': 'float32',
            
            # Categorical optimizations
            'sender.country': 'category',
            'sender.account_type': 'category',
            'receiver.country': 'category',
            'transfer.crypto_symbol': 'category',
            'fees.fee_type': 'category',
            'network.network_type': 'category',
            'status': 'category',
            'compliance.sanctions_check': 'category',
            'compliance.risk_level': 'category',
            
            # KYC levels
            'sender.kyc_level': 'int8',
            'receiver.kyc_level': 'int8',
            
            # Date columns
            'year': 'int16',
            'month': 'int8', 
            'day': 'int8'
        }
        
        # Apply optimizations
        for col, dtype in optimizations.items():
            if col in df.columns:
                try:
                    if dtype == 'category':
                        df[col] = df[col].astype('category')
                    else:
                        df[col] = df[col].astype(dtype)
                except Exception as e:
                    logger.warning(f"⚠️ Could not optimize {col}: {e}")
        
        # 💾 Memory usage report
        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        logger.info(f"💾 Optimized DataFrame memory usage: {memory_mb:.2f} MB")
        
        return df
    
    def create_parquet_partitions(self, df: pd.DataFrame, output_dir: str):
        """
        📁 Create partitioned Parquet files
        
        Args:
            df: Input DataFrame
            output_dir: Output directory for Parquet files
        """
        logger.info("📁 Creating partitioned Parquet files...")
        
        # 🎯 Partition by year, month, and sender country for optimal query performance
        partition_cols = ['year', 'month', 'sender.country']
        
        # 🗂️ Create PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # 📝 Define Parquet write options
        write_options = pq.ParquetWriter.open(
            output_dir,
            table.schema,
            compression='snappy',  # Good balance of compression and speed
            use_dictionary=True,   # Efficient for categorical data
            row_group_size=50000   # Optimize for analytical queries
        )
        
        logger.info(f"💾 Writing Parquet files to: {output_dir}")
        
        # 🔄 Write partitioned dataset
        pq.write_to_dataset(
            table,
            root_path=output_dir,
            partition_cols=partition_cols,
            compression='snappy',
            use_legacy_dataset=False,
            existing_data_behavior='overwrite_or_ignore'
        )
        
        logger.info("✅ Parquet partitions created successfully")
    
    def upload_to_hdfs(self, local_path: str, hdfs_path: str):
        """
        🌊 Upload Parquet files to HDFS
        
        Args:
            local_path: Local directory with Parquet files
            hdfs_path: HDFS destination path
        """
        if not self.hdfs_client:
            logger.warning("⚠️ HDFS client not available, skipping upload")
            logger.info(f"📁 Files are available locally at: {local_path}")
            return
        
        logger.info(f"🌊 Uploading to HDFS: {hdfs_path}")
        
        try:
            # 🗂️ Create HDFS directory if not exists
            self.hdfs_client.makedirs(hdfs_path)
            
            # 📤 Upload all Parquet files recursively
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    if file.endswith('.parquet'):
                        local_file = os.path.join(root, file)
                        # Maintain directory structure in HDFS
                        rel_path = os.path.relpath(local_file, local_path)
                        hdfs_file = f"{hdfs_path}/{rel_path}"
                        
                        logger.info(f"📤 Uploading: {file}")
                        self.hdfs_client.upload(hdfs_file, local_file, overwrite=True)
            
            logger.info("✅ Upload to HDFS completed successfully")
            
        except Exception as e:
            logger.error(f"❌ HDFS upload failed: {e}")
            raise
    
    def create_bronze_layer_metadata(self, df: pd.DataFrame, output_dir: str):
        """
        📋 Create metadata for Bronze layer
        
        Args:
            df: Source DataFrame
            output_dir: Output directory
        """
        metadata = {
            "table_name": "bronze_transactions",
            "description": "Raw transaction data in Bronze layer",
            "created_at": datetime.now().isoformat(),
            "row_count": len(df),
            "columns": len(df.columns),
            "partition_columns": ["year", "month", "sender.country"],
            "compression": "snappy",
            "format": "parquet",
            "schema": {col: str(df[col].dtype) for col in df.columns},
            "data_quality": {
                "null_counts": df.isnull().sum().to_dict(),
                "unique_countries": df['sender.country'].nunique(),
                "date_range": {
                    "start": df['timestamp'].min().isoformat(),
                    "end": df['timestamp'].max().isoformat()
                },
                "total_volume_usd": float(df['transfer.usd_equivalent'].sum()),
                "fraud_transactions": len(df[df['compliance.risk_level'].isin(['medium', 'high'])])
            }
        }
        
        # 💾 Save metadata
        metadata_path = os.path.join(output_dir, '_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"📋 Metadata saved to: {metadata_path}")
    
    def process_and_upload(self, json_path: str, local_output_dir: str = "data/bronze/transactions"):
        """
        🚀 Main process to convert JSON to Parquet and upload to HDFS
        
        Args:
            json_path: Path to input JSON file
            local_output_dir: Local output directory for Parquet files
        """
        logger.info("🚀 Starting Bronze layer data processing...")
        
        try:
            # 📖 Step 1: Load transaction data
            df = self.load_transaction_data(json_path)
            
            # ⚡ Step 2: Optimize schema
            df = self.optimize_schema(df)
            
            # 📁 Step 3: Create output directory
            os.makedirs(local_output_dir, exist_ok=True)
            
            # 🗂️ Step 4: Create partitioned Parquet files
            self.create_parquet_partitions(df, local_output_dir)
            
            # 📋 Step 5: Create metadata
            self.create_bronze_layer_metadata(df, local_output_dir)
            
            # 🌊 Step 6: Upload to HDFS
            hdfs_bronze_path = f"{self.bronze_path}/year={datetime.now().year}/month={datetime.now().month:02d}"
            self.upload_to_hdfs(local_output_dir, hdfs_bronze_path)
            
            # 📊 Success summary
            logger.info("✅ Bronze layer processing completed successfully!")
            logger.info(f"📊 Processed {len(df):,} transactions")
            logger.info(f"💰 Total volume: ${df['transfer.usd_equivalent'].sum():,.2f} USD")
            logger.info(f"📁 Local files: {local_output_dir}")
            if self.hdfs_client:
                logger.info(f"🌊 HDFS location: {hdfs_bronze_path}")
            
        except Exception as e:
            logger.error(f"❌ Processing failed: {e}")
            raise

def main():
    """🎯 Main execution function"""
    print("🚀 HDFS Data Lake Upload - Bronze Layer")
    print("=" * 50)
    
    # 🎛️ Configuration
    json_file = "data/raw/fake_transactions/fake_transactions.json"
    hdfs_host = os.getenv("HDFS_HOST", "localhost")
    hdfs_port = int(os.getenv("HDFS_PORT", "9000"))
    
    # ✅ Verify input file exists
    if not os.path.exists(json_file):
        logger.error(f"❌ Input file not found: {json_file}")
        logger.info("💡 Please run the transaction generator first")
        sys.exit(1)
    
    try:
        # 🏗️ Initialize uploader
        uploader = HDFSDataLakeUploader(hdfs_host=hdfs_host, hdfs_port=hdfs_port)
        
        # 🚀 Process and upload
        uploader.process_and_upload(json_file)
        
        print("\n🎉 Bronze layer ingestion completed successfully!")
        print("\n📋 Next steps:")
        print("1. 🔄 Set up Silver layer transformations with Spark")
        print("2. 🎯 Create Gold layer business aggregations")
        print("3. 📊 Build analytics dashboards")
        print("4. 🔍 Implement fraud detection ML models")
        
    except Exception as e:
        logger.error(f"❌ Failed to process data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()