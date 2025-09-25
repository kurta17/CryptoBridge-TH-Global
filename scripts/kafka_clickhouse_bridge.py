#!/usr/bin/env python3
"""
🌉 Simple Kafka-to-ClickHouse Bridge
Bypasses compression issues by using kafka-console-consumer
"""

import json
import subprocess
import sys
from datetime import datetime

try:
    import clickhouse_connect
except ImportError:
    print("❌ Please install: pip install clickhouse-connect")
    sys.exit(1)

def consume_and_insert():
    """Consume from Kafka and insert into ClickHouse"""
    
    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="analytics", 
        password="analytics123",
        database="cryptobridge"
    )
    print("✅ Connected to ClickHouse")
    
    # Use docker exec to consume from Kafka (bypasses compression)
    cmd = [
        "docker", "exec", "kafka", 
        "kafka-console-consumer",
        "--bootstrap-server", "localhost:9092",
        "--topic", "transactions-normal",
        "--from-beginning",
        "--max-messages", "10"  # Process 10 messages as test
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            print(f"❌ Kafka consumer failed: {result.stderr}")
            return False
            
        messages = result.stdout.strip().split('\n')
        processed = 0
        
        for msg_line in messages:
            if not msg_line or "Processed a total of" in msg_line:
                continue
                
            try:
                # Parse JSON message
                data = json.loads(msg_line)
                
                # Extract nested data
                sender = data.get('sender', {})
                receiver = data.get('receiver', {})
                transfer = data.get('transfer', {})
                compliance = data.get('compliance', {})
                
                # Insert into ClickHouse using correct method
                client.insert(
                    'transactions',
                    [{
                        'transaction_id': data.get('transaction_id', ''),
                        'user_id': sender.get('user_id', ''),
                        'amount': float(transfer.get('usd_equivalent', 0)),
                        'currency': transfer.get('crypto_symbol', 'USD'),
                        'transaction_type': 'crypto_transfer',
                        'status': data.get('status', 'unknown'),
                        'timestamp': datetime.fromisoformat(data.get('timestamp').replace('Z', '+00:00') if data.get('timestamp') else datetime.now().isoformat()),
                        'from_country': sender.get('country', ''),
                        'to_country': receiver.get('country', ''),
                        'fraud_score': float(compliance.get('aml_score', 0)),
                        'is_suspicious': compliance.get('risk_level', 'low') in ['medium', 'high'],
                        'topic_source': 'kafka_bridge'
                    }],
                    column_names=['transaction_id', 'user_id', 'amount', 'currency', 'transaction_type', 
                                 'status', 'timestamp', 'from_country', 'to_country', 'fraud_score', 
                                 'is_suspicious', 'topic_source']
                )
                processed += 1
                print(f"✅ Processed transaction {processed}: {data.get('transaction_id', 'unknown')}")
                
            except json.JSONDecodeError:
                print(f"⚠️ Skipping invalid JSON: {msg_line[:100]}...")
                continue
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                continue
        
        print(f"🎉 Successfully processed {processed} transactions!")
        return True
        
    except subprocess.TimeoutExpired:
        print("⏰ Consumer timed out")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🌉 Starting Kafka-to-ClickHouse Bridge")
    print("="*50)
    
    success = consume_and_insert()
    
    if success:
        print("\n🎯 Check your data with:")
        print("docker exec -it clickhouse clickhouse-client --user analytics --password analytics123 --database cryptobridge --query \"SELECT COUNT(*) FROM transactions\"")
    else:
        print("\n❌ Bridge failed. Check Docker containers are running.")