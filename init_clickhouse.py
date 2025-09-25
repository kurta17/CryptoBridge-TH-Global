#!/usr/bin/env python3
"""
🗄️ ClickHouse Database Initialization
Setup ClickHouse database and tables for CryptoBridge analytics
"""

import requests
import sys
import time

def initialize_clickhouse():
    """Initialize ClickHouse database and tables"""
    print("🗄️ Initializing ClickHouse Database")
    print("=" * 40)
    
    base_url = "http://localhost:8123/"
    auth_params = {
        'user': 'analytics',
        'password': 'analytics123'
    }
    
    # Step 1: Create database
    print("📁 Creating database 'cryptobridge'...")
    try:
        response = requests.get(
            base_url,
            params={
                **auth_params,
                'query': 'CREATE DATABASE IF NOT EXISTS cryptobridge'
            },
            timeout=10
        )
        if response.status_code == 200:
            print("✅ Database created successfully")
        else:
            print(f"⚠️ Database creation response: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Database creation failed: {e}")
        return False
    
    # Step 2: Create transactions table
    print("📊 Creating transactions table...")
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cryptobridge.transactions (
        transaction_id String,
        timestamp DateTime64(3),
        from_exchange String,
        to_exchange String,
        amount Float64,
        currency String,
        is_suspicious UInt8,
        risk_score UInt32,
        country String,
        account_type String,
        sender_user_id String,
        receiver_user_id String,
        crypto_currency String,
        fiat_amount Float64,
        exchange_rate Float64,
        transaction_type String,
        processing_time_ms UInt32,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (timestamp, transaction_id)
    SETTINGS index_granularity = 8192
    """
    
    try:
        response = requests.get(
            base_url,
            params={
                **auth_params,
                'query': create_table_query
            },
            timeout=10
        )
        if response.status_code == 200:
            print("✅ Transactions table created successfully")
        else:
            print(f"⚠️ Table creation response: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Table creation failed: {e}")
        return False
    
    # Step 3: Verify table exists and show structure
    print("🔍 Verifying table structure...")
    try:
        response = requests.get(
            base_url,
            params={
                **auth_params,
                'query': 'DESCRIBE cryptobridge.transactions FORMAT JSON'
            },
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Table verified! Structure:")
            for row in result.get('data', [])[:5]:  # Show first 5 columns
                print(f"   • {row['name']}: {row['type']}")
            if len(result.get('data', [])) > 5:
                print(f"   ... and {len(result['data']) - 5} more columns")
        else:
            print(f"⚠️ Table verification failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Table verification error: {e}")
    
    # Step 4: Test insert capability
    print("🧪 Testing insert capability...")
    test_insert_query = """
    INSERT INTO cryptobridge.transactions (
        transaction_id, timestamp, from_exchange, to_exchange, 
        amount, currency, is_suspicious, risk_score, country, account_type
    ) VALUES (
        'test-' || toString(now()), now(), 'test-exchange', 'target-exchange',
        100.0, 'USD', 0, 10, 'US', 'premium'
    )
    """
    
    try:
        response = requests.get(
            base_url,
            params={
                **auth_params,
                'query': test_insert_query
            },
            timeout=5
        )
        
        if response.status_code == 200:
            print("✅ Test insert successful")
            
            # Count rows
            response = requests.get(
                base_url,
                params={
                    **auth_params,
                    'query': 'SELECT COUNT(*) as count FROM cryptobridge.transactions FORMAT JSON'
                },
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                count = result['data'][0]['count']
                print(f"📊 Current row count: {count}")
            
        else:
            print(f"⚠️ Test insert failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Test insert error: {e}")
    
    print("\n🎉 ClickHouse initialization complete!")
    print("🔗 Access ClickHouse at: http://localhost:8123")
    print("📊 Database: cryptobridge")
    print("📋 Table: transactions")
    print("👤 User: analytics / Password: analytics123")
    
    return True

if __name__ == "__main__":
    success = initialize_clickhouse()
    sys.exit(0 if success else 1)