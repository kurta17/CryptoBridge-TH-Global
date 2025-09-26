#!/usr/bin/env python3
"""
Quick Data Loader for ClickHouse - Basic data to get dashboards working
"""

import requests
import json
import random
from datetime import datetime, timedelta

# ClickHouse connection details
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"
CLICKHOUSE_DB = "cryptobridge"

def execute_clickhouse_query(query, data=None):
    """Execute a query in ClickHouse"""
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "database": CLICKHOUSE_DB
    }
    
    # Always use POST for ClickHouse queries to avoid readonly mode
    response = requests.post(url, params=params, data=query, headers={"Content-Type": "text/plain"})
    
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return None
    
    return response.text

def create_tables():
    """Create basic tables structure"""
    print("Creating database and tables...")
    
    # Create database
    execute_clickhouse_query("CREATE DATABASE IF NOT EXISTS cryptobridge")
    
    # Create transactions table
    transactions_table = """
    CREATE TABLE IF NOT EXISTS cryptobridge.transactions (
        transaction_id String,
        sender_id String,
        recipient_id String,
        sender_country String,
        recipient_country String,
        currency String,
        amount Float64,
        transaction_date Date,
        transaction_timestamp DateTime,
        is_suspicious UInt8,
        account_type String
    ) ENGINE = MergeTree()
    ORDER BY (transaction_date, transaction_id)
    """
    execute_clickhouse_query(transactions_table)
    
    # Create crypto prices table
    crypto_table = """
    CREATE TABLE IF NOT EXISTS cryptobridge.crypto_prices (
        symbol String,
        price_usd Float64,
        volume_24h Float64,
        percent_change_24h Float64,
        market_cap Float64,
        last_updated DateTime
    ) ENGINE = MergeTree()
    ORDER BY (symbol, last_updated)
    """
    execute_clickhouse_query(crypto_table)
    
    # Create user accounts table
    user_accounts_table = """
    CREATE TABLE IF NOT EXISTS cryptobridge.user_accounts (
        user_id String,
        account_type String,
        total_transaction_volume Float64,
        transaction_count UInt32,
        created_date Date
    ) ENGINE = MergeTree()
    ORDER BY user_id
    """
    execute_clickhouse_query(user_accounts_table)
    
    print("✅ Tables created successfully")

def insert_sample_data():
    """Insert basic sample data for dashboards to work"""
    print("Inserting sample data...")
    
    # Sample transaction data
    countries = ['US', 'UK', 'TH', 'SG', 'JP', 'AU', 'CA', 'DE', 'FR', 'HK']
    currencies = ['BTC', 'ETH', 'USDT', 'BNB', 'ADA', 'DOT']
    account_types = ['whale', 'regular', 'suspicious']
    
    transactions = []
    for i in range(1000):  # Create 1000 sample transactions
        tx_date = datetime.now() - timedelta(days=random.randint(0, 30))
        sender_country = random.choice(countries)
        recipient_country = random.choice(countries)
        while recipient_country == sender_country:
            recipient_country = random.choice(countries)
        
        account_type = random.choice(account_types)
        is_suspicious = 1 if account_type == 'suspicious' or random.random() < 0.05 else 0
        
        tx = {
            'transaction_id': f'tx_{i:06d}',
            'sender_id': f'user_{random.randint(1, 100):04d}',
            'recipient_id': f'user_{random.randint(1, 100):04d}',
            'sender_country': sender_country,
            'recipient_country': recipient_country,
            'currency': random.choice(currencies),
            'amount': round(random.uniform(100, 100000), 2),
            'transaction_date': tx_date.strftime('%Y-%m-%d'),
            'transaction_timestamp': tx_date.strftime('%Y-%m-%d %H:%M:%S'),
            'is_suspicious': is_suspicious,
            'account_type': account_type
        }
        transactions.append(tx)
    
    # Insert transactions
    values = []
    for tx in transactions:
        values.append(f"('{tx['transaction_id']}', '{tx['sender_id']}', '{tx['recipient_id']}', "
                     f"'{tx['sender_country']}', '{tx['recipient_country']}', '{tx['currency']}', "
                     f"{tx['amount']}, '{tx['transaction_date']}', '{tx['transaction_timestamp']}', "
                     f"{tx['is_suspicious']}, '{tx['account_type']}')")
    
    insert_query = f"""
    INSERT INTO cryptobridge.transactions VALUES
    {', '.join(values)}
    """
    execute_clickhouse_query(insert_query)
    print(f"✅ Inserted {len(transactions)} transactions")
    
    # Sample crypto prices
    crypto_data = [
        {'symbol': 'BTC', 'price_usd': 43250.50, 'volume_24h': 15000000000, 'percent_change_24h': 2.5, 'market_cap': 850000000000},
        {'symbol': 'ETH', 'price_usd': 2650.75, 'volume_24h': 8000000000, 'percent_change_24h': 3.2, 'market_cap': 320000000000},
        {'symbol': 'BNB', 'price_usd': 315.20, 'volume_24h': 1200000000, 'percent_change_24h': -1.8, 'market_cap': 48000000000},
        {'symbol': 'ADA', 'price_usd': 0.58, 'volume_24h': 450000000, 'percent_change_24h': 4.1, 'market_cap': 20000000000},
        {'symbol': 'DOT', 'price_usd': 7.85, 'volume_24h': 280000000, 'percent_change_24h': -0.5, 'market_cap': 10000000000},
        {'symbol': 'USDT', 'price_usd': 1.00, 'volume_24h': 25000000000, 'percent_change_24h': 0.01, 'market_cap': 95000000000}
    ]
    
    crypto_values = []
    for crypto in crypto_data:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        crypto_values.append(f"('{crypto['symbol']}', {crypto['price_usd']}, {crypto['volume_24h']}, "
                           f"{crypto['percent_change_24h']}, {crypto['market_cap']}, '{timestamp}')")
    
    crypto_insert = f"""
    INSERT INTO cryptobridge.crypto_prices VALUES
    {', '.join(crypto_values)}
    """
    execute_clickhouse_query(crypto_insert)
    print(f"✅ Inserted {len(crypto_data)} crypto prices")
    
    # Sample user accounts
    users = set()
    for tx in transactions:
        users.add(tx['sender_id'])
        users.add(tx['recipient_id'])
    
    user_values = []
    for user_id in users:
        user_txs = [tx for tx in transactions if tx['sender_id'] == user_id or tx['recipient_id'] == user_id]
        total_volume = sum(tx['amount'] for tx in user_txs if tx['sender_id'] == user_id)
        account_type = random.choice(account_types)
        
        user_values.append(f"('{user_id}', '{account_type}', {total_volume}, "
                          f"{len([tx for tx in user_txs if tx['sender_id'] == user_id])}, "
                          f"'{(datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')}')")
    
    users_insert = f"""
    INSERT INTO cryptobridge.user_accounts VALUES
    {', '.join(user_values)}
    """
    execute_clickhouse_query(users_insert)
    print(f"✅ Inserted {len(users)} user accounts")

def main():
    """Main function"""
    print("🚀 Quick Data Loader for CryptoBridge Analytics")
    print("=" * 50)
    
    create_tables()
    insert_sample_data()
    
    # Verify data
    tx_count = execute_clickhouse_query("SELECT COUNT(*) FROM cryptobridge.transactions")
    crypto_count = execute_clickhouse_query("SELECT COUNT(*) FROM cryptobridge.crypto_prices")
    user_count = execute_clickhouse_query("SELECT COUNT(*) FROM cryptobridge.user_accounts")
    
    print("\n📊 Data Summary:")
    print(f"   Transactions: {tx_count.strip()}")
    print(f"   Crypto Prices: {crypto_count.strip()}")
    print(f"   User Accounts: {user_count.strip()}")
    print("\n✅ Quick data loading complete! ClickHouse is ready for Metabase.")

if __name__ == "__main__":
    main()