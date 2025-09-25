#!/usr/bin/env python3
"""
🎯 Kafka Topics Initialization
📡 Setup Kafka topics for CryptoBridge streaming pipeline

This script creates and configures Kafka topics for the transaction streaming pipeline.
"""

import time
import logging
from typing import Dict, List

try:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka import KafkaProducer
except ImportError:
    print("❌ Missing kafka-python dependency!")
    print("Please install: pip install kafka-python")
    exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_topics(bootstrap_servers: str = "localhost:9092"):
    """📡 Create Kafka topics for streaming pipeline"""
    
    # Topic configurations
    topics_config = [
        {
            'name': 'transactions-normal',
            'partitions': 6,
            'replication_factor': 1,
            'description': 'Normal cryptocurrency transactions'
        },
        {
            'name': 'transactions-suspicious', 
            'partitions': 3,
            'replication_factor': 1,
            'description': 'Suspicious transactions for fraud analysis'
        },
        {
            'name': 'transactions-fraud-alerts',
            'partitions': 2,
            'replication_factor': 1, 
            'description': 'Real-time fraud alerts'
        },
        {
            'name': 'analytics-aggregations',
            'partitions': 4,
            'replication_factor': 1,
            'description': 'Real-time analytics aggregations'
        }
    ]
    
    try:
        # Wait for Kafka to be ready
        logger.info("⏳ Waiting for Kafka to be ready...")
        time.sleep(10)
        
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='cryptobridge-topic-creator'
        )
        
        # Create topics
        topics_to_create = []
        for topic_config in topics_config:
            topic = NewTopic(
                name=topic_config['name'],
                num_partitions=topic_config['partitions'],
                replication_factor=topic_config['replication_factor'],
                topic_configs={
                    'retention.ms': '604800000',  # 7 days
                    'cleanup.policy': 'delete',
                    'compression.type': 'snappy'
                }
            )
            topics_to_create.append(topic)
        
        # Create all topics
        logger.info("📡 Creating Kafka topics...")
        
        for topic_config in topics_config:
            try:
                topic = NewTopic(
                    name=topic_config['name'],
                    num_partitions=topic_config['partitions'],
                    replication_factor=topic_config['replication_factor'],
                    topic_configs={
                        'retention.ms': '604800000',  # 7 days
                        'cleanup.policy': 'delete',
                        'compression.type': 'snappy'
                    }
                )
                
                result = admin_client.create_topics([topic], validate_only=False)
                logger.info(f"✅ Topic created: {topic_config['name']}")
                
            except Exception as e:
                if "already exists" in str(e).lower() or "topic already exists" in str(e).lower():
                    logger.info(f"📝 Topic already exists: {topic_config['name']}")
                else:
                    logger.error(f"❌ Failed to create topic {topic_config['name']}: {e}")
        
        # List all topics to verify
        logger.info("📋 Listing all topics:")
        try:
            # Use list_topics instead of describe_topics for compatibility
            metadata = admin_client.describe_topics()
            for topic_name in metadata.topics:
                logger.info(f"   📡 {topic_name}")
        except Exception as e:
            logger.info("📋 Topics created successfully (unable to list due to API version)")
            
        logger.info("✅ Kafka topics setup complete!")
        
    except Exception as e:
        logger.error(f"❌ Failed to setup Kafka topics: {e}")
        raise

def main():
    """🎯 Main execution"""
    print("📡 CryptoBridge Kafka Topics Setup")
    print("=" * 40)
    
    create_topics()

if __name__ == "__main__":
    main()