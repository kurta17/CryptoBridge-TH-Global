#!/usr/bin/env python3
"""
Simple Kafka Consumer Test
"""

import json
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_consumer():
    """Test consuming messages from Kafka topics"""
    print("🔍 Testing Kafka Consumer")
    print("=" * 40)
    
    consumer = KafkaConsumer(
        'transactions-normal',
        'transactions-suspicious', 
        'transactions-fraud-alerts',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='test-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("✅ Consumer connected, listening for messages...")
    print("Press Ctrl+C to stop")
    
    message_count = 0
    try:
        for message in consumer:
            message_count += 1
            data = message.value
            
            print(f"\n📨 Message #{message_count}")
            print(f"   Topic: {message.topic}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Transaction ID: {data.get('transaction_id', 'N/A')}")
            print(f"   From: {data.get('from_exchange', 'N/A')}")
            print(f"   To: {data.get('to_exchange', 'N/A')}")
            print(f"   Amount: {data.get('amount', 'N/A')} {data.get('currency', 'N/A')}")
            print(f"   Suspicious: {data.get('is_suspicious', 'N/A')}")
            
            if message_count >= 10:
                print(f"\n✅ Successfully received {message_count} messages!")
                break
                
    except KeyboardInterrupt:
        print(f"\n🛑 Consumer stopped. Received {message_count} messages total.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    test_consumer()