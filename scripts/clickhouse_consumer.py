#!/usr/bin/env python3
"""
🔄 Kafka to ClickHouse Consumer
Real-time transaction streaming from Kafka to ClickHouse

This consumer reads transaction data from Kafka topics and stores them in ClickHouse
for real-time analytics and fraud detection.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List
import sys

try:
    from kafka import KafkaConsumer
    import clickhouse_connect
except ImportError as e:
    print("❌ Missing required dependencies!")
    print("Please install: pip install kafka-python clickhouse-connect")
    print(f"Error: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ClickHouseTransactionConsumer:
    def __init__(self, 
                 kafka_bootstrap_servers: str = "localhost:9092",
                 clickhouse_host: str = "localhost",
                 clickhouse_port: int = 8123,
                 clickhouse_user: str = "analytics",
                 clickhouse_password: str = "analytics123",
                 clickhouse_database: str = "cryptobridge"):
        
        self.kafka_servers = kafka_bootstrap_servers
        self.topics = [
            'transactions-normal',
            'transactions-suspicious', 
            'transactions-fraud-alerts'
        ]
        
        # Initialize ClickHouse connection
        try:
            self.clickhouse_client = clickhouse_connect.get_client(
                host=clickhouse_host,
                port=clickhouse_port,
                username=clickhouse_user,
                password=clickhouse_password,
                database=clickhouse_database
            )
            logger.info("✅ Connected to ClickHouse")
        except Exception as e:
            logger.error(f"❌ Failed to connect to ClickHouse: {e}")
            raise
        
        # Initialize Kafka consumer
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='clickhouse-consumer-group',
                auto_offset_reset='earliest',  # Process from beginning
                enable_auto_commit=True,
                consumer_timeout_ms=30000,     # Longer timeout
                fetch_max_wait_ms=1000
            )
            logger.info("✅ Connected to Kafka")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
        
        # Create ClickHouse tables
        self.create_tables()
    
    def create_tables(self):
        """Create ClickHouse tables for transaction data"""
        try:
            # Main transactions table
            self.clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id String,
                    user_id String,
                    amount Decimal(18, 2),
                    currency String,
                    transaction_type String,
                    status String,
                    timestamp DateTime64(3),
                    from_country String,
                    to_country String,
                    fraud_score Float32,
                    is_suspicious Bool,
                    topic_source String,
                    created_at DateTime64(3) DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (timestamp, transaction_id)
                PARTITION BY toYYYYMM(timestamp)
            """)
            
            # Fraud alerts table
            self.clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS fraud_alerts (
                    alert_id String,
                    transaction_id String,
                    user_id String,
                    fraud_score Float32,
                    alert_type String,
                    severity String,
                    timestamp DateTime64(3),
                    details String,
                    created_at DateTime64(3) DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (timestamp, alert_id)
                PARTITION BY toYYYYMM(timestamp)
            """)
            
            logger.info("✅ ClickHouse tables created/verified")
            
        except Exception as e:
            logger.error(f"❌ Failed to create ClickHouse tables: {e}")
            raise
    
    def process_transaction(self, message: Dict[str, Any], topic: str):
        """Process and insert a single transaction"""
        try:
            # Parse transaction data with the actual structure from Kafka
            data = message
            
            # Extract nested data
            sender = data.get('sender', {})
            receiver = data.get('receiver', {})
            transfer = data.get('transfer', {})
            compliance = data.get('compliance', {})
            
            # Safely extract numeric values
            usd_amount = transfer.get('usd_equivalent', 0.0)
            if usd_amount == 0:
                logger.warning(f"⚠️ Zero USD amount for transaction {data.get('transaction_id', 'unknown')}")
            
            aml_score = compliance.get('aml_score', 0.0)
            
            # Safe timestamp handling
            timestamp_str = data.get('timestamp', datetime.now().isoformat())
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except ValueError:
                logger.warning(f"⚠️ Invalid timestamp format: {timestamp_str}, using current time")
                timestamp = datetime.now()
            
            # Insert into transactions table - use list format for ClickHouse
            # Table columns: ['transaction_id', 'user_id', 'amount', 'currency', 'transaction_type', 'status', 'timestamp', 'from_country', 'to_country', 'fraud_score', 'is_suspicious', 'topic_source', 'created_at']
            transaction_row = [
                data.get('transaction_id', ''),                                          # transaction_id
                sender.get('user_id', ''),                                               # user_id  
                float(usd_amount),                                                       # amount
                transfer.get('crypto_symbol', 'USD'),                                    # currency
                'crypto_transfer',                                                       # transaction_type
                data.get('status', 'unknown'),                                           # status
                timestamp,                                                               # timestamp
                sender.get('country', ''),                                               # from_country
                receiver.get('country', ''),                                             # to_country
                float(aml_score),                                                        # fraud_score
                compliance.get('risk_level', 'low') in ['medium', 'high'],              # is_suspicious
                topic,                                                                   # topic_source
                datetime.now()                                                           # created_at
            ]
            
            self.clickhouse_client.insert('transactions', [transaction_row])
            
            # If it's a fraud alert, also insert into fraud_alerts table
            if topic == 'transactions-fraud-alerts' or aml_score > 0.7:
                # Insert into fraud_alerts table - use list format for ClickHouse
                # fraud_alerts columns: ['alert_id', 'transaction_id', 'user_id', 'fraud_score', 'alert_type', 'severity', 'timestamp', 'details', 'created_at']
                fraud_alert_row = [
                    f"alert_{data.get('transaction_id', '')}",                           # alert_id
                    data.get('transaction_id', ''),                                      # transaction_id
                    sender.get('user_id', ''),                                           # user_id
                    float(aml_score),                                                    # fraud_score
                    'fraud_detection',                                                   # alert_type
                    'HIGH' if aml_score > 0.8 else 'MEDIUM',                            # severity
                    timestamp,                                                           # timestamp
                    json.dumps(data),                                                    # details
                    datetime.now()                                                       # created_at
                ]
                
                self.clickhouse_client.insert('fraud_alerts', [fraud_alert_row])
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to process transaction: {e}")
            # Log the actual data for debugging
            logger.error(f"💀 Problem data: {json.dumps(message, indent=2, default=str)}")
            return False
    
    def start_consuming(self):
        """Start consuming messages from Kafka and inserting into ClickHouse"""
        logger.info(f"🚀 Starting to consume from topics: {self.topics}")
        
        try:
            processed_count = 0
            
            for message in self.consumer:
                try:
                    success = self.process_transaction(message.value, message.topic)
                    
                    if success:
                        processed_count += 1
                        if processed_count % 10 == 0:
                            logger.info(f"✅ Processed {processed_count} transactions")
                    
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped by user")
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}")
        finally:
            self.consumer.close()
            logger.info(f"📊 Total transactions processed: {processed_count}")

def main():
    """Main function to start the consumer"""
    consumer = ClickHouseTransactionConsumer()
    
    # Show current table status
    try:
        tables = consumer.clickhouse_client.query("SHOW TABLES").result_rows
        logger.info(f"📊 Available tables: {[t[0] for t in tables]}")
        
        # Show transaction count
        count = consumer.clickhouse_client.query("SELECT COUNT(*) FROM transactions").result_rows[0][0]
        logger.info(f"📈 Current transaction count: {count}")
        
    except Exception as e:
        logger.warning(f"⚠️ Could not check table status: {e}")
    
    # Start consuming
    consumer.start_consuming()

if __name__ == "__main__":
    main()