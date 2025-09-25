#!/usr/bin/env python3
"""
Simple ClickHouse Consumer for Testing
Consumes messages from Kafka and inserts them into ClickHouse
"""

import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
import clickhouse_connect

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ClickHouseConsumer:
    def __init__(self):
        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'group_id': 'clickhouse-consumer-test',
            'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
        }
        
        # ClickHouse configuration
        self.clickhouse_client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='analytics',
            password='analytics123'
        )
        
        # Topics to consume
        self.topics = [
            'crypto-transactions',
            'high-risk-transactions',
            'low-risk-transactions'
        ]
        
        logger.info(f"🔧 Configured consumer for topics: {self.topics}")
        
    def consume_messages(self):
        """Consume messages from Kafka and insert into ClickHouse"""
        try:
            consumer = KafkaConsumer(*self.topics, **self.kafka_config)
            logger.info("🚀 Started consuming messages...")
            
            message_count = 0
            batch_size = 10
            batch = []
            
            for message in consumer:
                try:
                    data = message.value
                    topic = message.topic
                    
                    logger.info(f"📨 Received from {topic}: {data['transaction_id']}")
                    
                    # Prepare data for ClickHouse insertion
                    insert_sql = """
                    INSERT INTO cryptobridge.transactions 
                    (transaction_id, timestamp, from_exchange, to_exchange, amount, currency, 
                     is_suspicious, risk_score, country, account_type) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    # Convert timestamp to proper format
                    timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                    
                    values = [
                        data['transaction_id'],
                        timestamp,
                        data['from_exchange'],
                        data['to_exchange'],
                        data['amount'],
                        data['currency'],
                        1 if data.get('is_suspicious', False) else 0,
                        data.get('risk_score', 0),
                        data.get('country', 'Unknown'),
                        data.get('account_type', 'standard')
                    ]
                    
                    # Insert into ClickHouse
                    self.clickhouse_client.command(
                        "INSERT INTO cryptobridge.transactions VALUES",
                        [values]
                    )
                    
                    message_count += 1
                    logger.info(f"✅ Inserted transaction {data['transaction_id']} (Total: {message_count})")
                    
                    # Show stats every 10 messages
                    if message_count % 10 == 0:
                        result = self.clickhouse_client.query("SELECT COUNT(*) FROM cryptobridge.transactions")
                        total_records = result.result_rows[0][0]
                        logger.info(f"📊 Total records in ClickHouse: {total_records}")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped by user")
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}")
        finally:
            logger.info(f"📊 Final stats: {message_count} messages processed")
            
    def show_current_data(self):
        """Show current data in ClickHouse"""
        try:
            result = self.clickhouse_client.query("SELECT COUNT(*) FROM cryptobridge.transactions")
            count = result.result_rows[0][0]
            logger.info(f"📊 Current records in ClickHouse: {count}")
            
            if count > 0:
                result = self.clickhouse_client.query(
                    "SELECT transaction_id, timestamp, from_exchange, to_exchange, amount, currency, risk_score "
                    "FROM cryptobridge.transactions ORDER BY timestamp DESC LIMIT 5"
                )
                logger.info("📋 Latest 5 transactions:")
                for row in result.result_rows:
                    logger.info(f"   {row[0]} | {row[1]} | {row[2]}→{row[3]} | {row[4]} {row[5]} | Risk:{row[6]}")
                    
        except Exception as e:
            logger.error(f"❌ Error querying data: {e}")

if __name__ == "__main__":
    consumer = ClickHouseConsumer()
    
    logger.info("🎭 CryptoBridge ClickHouse Consumer Test")
    logger.info("======================================")
    
    # Show current data
    consumer.show_current_data()
    
    # Start consuming
    logger.info("Starting consumer... Press Ctrl+C to stop")
    consumer.consume_messages()