#!/usr/bin/env python3
"""
🗄️ ClickHouse Data Loader for CryptoBridge Analytics
Loads generated fake transactions and real crypto data into ClickHouse
for comprehensive business intelligence analysis.
"""

import json
import sys
import os
import time
from datetime import datetime
from typing import List, Dict, Any
import requests
import asyncio

class ClickHouseLoader:
    def __init__(self):
        """Initialize ClickHouse loader"""
        self.base_url = "http://localhost:8123/"
        self.auth_params = {
            'user': 'analytics',
            'password': 'analytics123'
        }
        self.database = 'cryptobridge'
        
        print("🗄️ ClickHouse Data Loader initialized")
    
    def setup_enhanced_schema(self):
        """Set up enhanced database schema for business analytics"""
        print("🔧 Setting up enhanced database schema...")
        
        # Create database
        try:
            response = requests.post(
                self.base_url,
                params=self.auth_params,
                data=f'CREATE DATABASE IF NOT EXISTS {self.database}',
                timeout=10
            )
            if response.status_code == 200:
                print(f"✅ Database '{self.database}' ready")
            else:
                print(f"⚠️ Database response: {response.status_code}")
        except Exception as e:
            print(f"❌ Database creation failed: {e}")
            return False
        
        # Enhanced transactions table for business analytics
        transactions_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.transactions (
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
            sender_country String,
            receiver_country String,
            sender_kyc_level UInt8,
            receiver_kyc_level UInt8,
            thb_equivalent Float64,
            fee_usd Float64,
            network_confirmations UInt8,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp, transaction_id, sender_user_id)
        SETTINGS index_granularity = 8192
        """
        
        # Crypto prices table for real-time price data
        crypto_prices_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.crypto_prices (
            symbol String,
            price Float64,
            timestamp DateTime64(3),
            source String,
            volume_24h Float64,
            price_change_24h Float64,
            market_cap Float64,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp)
        SETTINGS index_granularity = 8192
        """
        
        # Trading volumes table for exchange comparison
        trading_volumes_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.trading_volumes (
            exchange String,
            symbol String,
            daily_volume Float64,
            daily_trades UInt32,
            timestamp Date,
            price_change_pct Float64,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (exchange, symbol, timestamp)
        SETTINGS index_granularity = 8192
        """
        
        # User accounts table for relationship analysis
        user_accounts_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.user_accounts (
            user_id String,
            account_type String,
            email String,
            country String,
            exchange String,
            kyc_level UInt8,
            registration_date Date,
            risk_profile String,
            monthly_volume_usd Float64,
            preferred_cryptos Array(String),
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (user_id, exchange)
        SETTINGS index_granularity = 8192
        """
        
        tables = [
            ("transactions", transactions_table),
            ("crypto_prices", crypto_prices_table),
            ("trading_volumes", trading_volumes_table),
            ("user_accounts", user_accounts_table)
        ]
        
        for table_name, table_sql in tables:
            try:
                response = requests.post(
                    self.base_url,
                    params=self.auth_params,
                    data=table_sql,
                    timeout=15
                )
                if response.status_code == 200:
                    print(f"✅ Table '{table_name}' created/verified")
                else:
                    print(f"⚠️ Table '{table_name}' response: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"❌ Table '{table_name}' creation failed: {e}")
                return False
        
        return True
    
    def load_transactions(self, file_path: str) -> bool:
        """Load transaction data from JSON file"""
        print(f"📊 Loading transactions from {file_path}...")
        
        try:
            with open(file_path, 'r') as f:
                transactions = json.load(f)
            
            print(f"   📄 Loaded {len(transactions)} transactions from file")
            
            # Batch insert transactions
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(transactions), batch_size):
                batch = transactions[i:i + batch_size]
                
                # Prepare INSERT statement
                values = []
                for tx in batch:
                    # Handle potential None values
                    def safe_value(val, default=''):
                        return val if val is not None else default
                    
                    value_tuple = f"""(
                        '{safe_value(tx.get('transaction_id', ''))}',
                        '{safe_value(tx.get('timestamp', datetime.now().isoformat()))}',
                        '{safe_value(tx.get('from_exchange', 'Binance_Global'))}',
                        '{safe_value(tx.get('to_exchange', 'Binance_TH'))}',
                        {tx.get('amount', 0)},
                        '{safe_value(tx.get('currency', 'USDT'))}',
                        {tx.get('is_suspicious', 0)},
                        {tx.get('risk_score', 0)},
                        '{safe_value(tx.get('country', 'US'))}',
                        '{safe_value(tx.get('account_type', 'regular'))}',
                        '{safe_value(tx.get('sender_user_id', ''))}',
                        '{safe_value(tx.get('receiver_user_id', ''))}',
                        '{safe_value(tx.get('crypto_currency', tx.get('currency', 'USDT')))}',
                        {tx.get('fiat_amount', 0)},
                        {tx.get('exchange_rate', 1)},
                        '{safe_value(tx.get('transaction_type', 'cross_border_transfer'))}',
                        {tx.get('processing_time_ms', 1000)},
                        '{safe_value(tx.get('sender_country', tx.get('country', 'US')))}',
                        '{safe_value(tx.get('receiver_country', 'TH'))}',
                        {tx.get('sender_kyc_level', 2)},
                        {tx.get('receiver_kyc_level', 2)},
                        {tx.get('thb_equivalent', tx.get('fiat_amount', 0) * 35.1)},
                        {tx.get('fee_usd', tx.get('fiat_amount', 0) * 0.001)},
                        {tx.get('network_confirmations', 6)}
                    )"""
                    values.append(value_tuple)
                
                insert_sql = f"""
                INSERT INTO {self.database}.transactions (
                    transaction_id, timestamp, from_exchange, to_exchange, amount, currency,
                    is_suspicious, risk_score, country, account_type, sender_user_id, receiver_user_id,
                    crypto_currency, fiat_amount, exchange_rate, transaction_type, processing_time_ms,
                    sender_country, receiver_country, sender_kyc_level, receiver_kyc_level,
                    thb_equivalent, fee_usd, network_confirmations
                ) VALUES {', '.join(values)}
                """
                
                try:
                    response = requests.post(
                        self.base_url,
                        params=self.auth_params,
                        data=insert_sql,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        total_inserted += len(batch)
                        if i % (batch_size * 10) == 0:
                            print(f"   ⏳ Inserted {total_inserted:,}/{len(transactions):,} transactions...")
                    else:
                        print(f"⚠️ Batch insert failed: {response.status_code} - {response.text[:200]}")
                        
                except Exception as e:
                    print(f"❌ Batch insert error: {e}")
                    continue
            
            print(f"✅ Successfully loaded {total_inserted:,} transactions")
            return True
            
        except Exception as e:
            print(f"❌ Failed to load transactions: {e}")
            return False
    
    def load_crypto_prices(self, file_path: str) -> bool:
        """Load cryptocurrency price data"""
        print(f"💰 Loading crypto prices from {file_path}...")
        
        try:
            with open(file_path, 'r') as f:
                crypto_data = json.load(f)
            
            current_time = datetime.now().isoformat()
            
            # Load current prices
            if 'prices' in crypto_data:
                values = []
                for symbol, price in crypto_data['prices'].items():
                    value_tuple = f"('{symbol}', {price}, '{current_time}', 'binance_global', 0, 0, 0)"
                    values.append(value_tuple)
                
                if values:
                    insert_sql = f"""
                    INSERT INTO {self.database}.crypto_prices 
                    (symbol, price, timestamp, source, volume_24h, price_change_24h, market_cap)
                    VALUES {', '.join(values)}
                    """
                    
                    response = requests.post(
                        self.base_url,
                        params=self.auth_params,
                        data=insert_sql,
                        timeout=15
                    )
                    
                    if response.status_code == 200:
                        print(f"✅ Loaded {len(values)} price points")
                    else:
                        print(f"⚠️ Price loading failed: {response.status_code}")
            
            # Load THB pairs
            if 'thb_pairs' in crypto_data:
                values = []
                for symbol, price in crypto_data['thb_pairs'].items():
                    value_tuple = f"('{symbol}', {price}, '{current_time}', 'binance_th', 0, 0, 0)"
                    values.append(value_tuple)
                
                if values:
                    insert_sql = f"""
                    INSERT INTO {self.database}.crypto_prices 
                    (symbol, price, timestamp, source, volume_24h, price_change_24h, market_cap)
                    VALUES {', '.join(values)}
                    """
                    
                    response = requests.post(
                        self.base_url,
                        params=self.auth_params,
                        data=insert_sql,
                        timeout=15
                    )
                    
                    if response.status_code == 200:
                        print(f"✅ Loaded {len(values)} THB pairs")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to load crypto prices: {e}")
            return False
    
    def load_trading_volumes(self, file_path: str) -> bool:
        """Load trading volume comparison data"""
        print(f"📈 Loading trading volumes from {file_path}...")
        
        try:
            with open(file_path, 'r') as f:
                trading_data = json.load(f)
            
            current_date = datetime.now().date().isoformat()
            
            # Load volume comparison data
            if 'volume_comparison' in trading_data:
                global_values = []
                th_values = []
                
                for symbol, data in trading_data['volume_comparison'].items():
                    # Global exchange data
                    global_tuple = f"""(
                        'Binance_Global', '{symbol}', {data.get('global_daily_volume', 0)},
                        {data.get('global_daily_trades', 0)}, '{current_date}', 
                        {data.get('price_change_24h', 0)}
                    )"""
                    global_values.append(global_tuple)
                    
                    # Thailand exchange data
                    th_tuple = f"""(
                        'Binance_TH', '{symbol}', {data.get('th_daily_volume', 0)},
                        {data.get('th_daily_trades', 0)}, '{current_date}', 
                        {data.get('price_change_24h', 0)}
                    )"""
                    th_values.append(th_tuple)
                
                all_values = global_values + th_values
                
                if all_values:
                    insert_sql = f"""
                    INSERT INTO {self.database}.trading_volumes 
                    (exchange, symbol, daily_volume, daily_trades, timestamp, price_change_pct)
                    VALUES {', '.join(all_values)}
                    """
                    
                    response = requests.post(
                        self.base_url,
                        params=self.auth_params,
                        data=insert_sql,
                        timeout=15
                    )
                    
                    if response.status_code == 200:
                        print(f"✅ Loaded {len(all_values)} trading volume records")
                    else:
                        print(f"⚠️ Trading volumes loading failed: {response.status_code}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to load trading volumes: {e}")
            return False
    
    def verify_data_loading(self):
        """Verify that data was loaded correctly"""
        print("🔍 Verifying data loading...")
        
        tables = ['transactions', 'crypto_prices', 'trading_volumes']
        
        for table in tables:
            try:
                response = requests.post(
                    self.base_url,
                    params=self.auth_params,
                    data=f'SELECT COUNT(*) as count FROM {self.database}.{table}',
                    timeout=10
                )
                
                if response.status_code == 200:
                    result = response.json()
                    count = result['data'][0]['count']
                    print(f"   📊 {table}: {count:,} records")
                else:
                    print(f"   ⚠️ {table}: Query failed")
                    
            except Exception as e:
                print(f"   ❌ {table}: Verification error - {e}")
        
        # Test business queries
        print("\n🎯 Testing business analytics queries...")
        
        # Test cross-border transfer volume
        try:
            response = requests.post(
                self.base_url,
                params=self.auth_params,
                data=f"""
                SELECT 
                    COUNT(*) as total_transactions,
                    SUM(fiat_amount) as total_usd_volume,
                    SUM(thb_equivalent) as total_thb_volume,
                    AVG(fiat_amount) as avg_transaction_usd
                FROM {self.database}.transactions
                """,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                data = result['data'][0]
                print(f"   ✅ Cross-border Analysis: {data['total_transactions']:,} transactions, "
                      f"${data['total_usd_volume']:,.2f} total volume")
            
        except Exception as e:
            print(f"   ❌ Cross-border query failed: {e}")
        
        # Test fraud detection
        try:
            response = requests.post(
                self.base_url,
                params=self.auth_params,
                data=f"""
                SELECT 
                    COUNT(*) as suspicious_count,
                    SUM(fiat_amount) as suspicious_volume_usd,
                    AVG(risk_score) as avg_risk_score
                FROM {self.database}.transactions
                WHERE is_suspicious = 1
                """,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                data = result['data'][0]
                print(f"   ✅ Fraud Detection: {data['suspicious_count']:,} suspicious transactions, "
                      f"${data['suspicious_volume_usd']:,.2f} suspicious volume")
            
        except Exception as e:
            print(f"   ❌ Fraud detection query failed: {e}")

def main():
    """Main execution function"""
    print("🚀 Starting ClickHouse Data Loading...")
    
    loader = ClickHouseLoader()
    
    # Set up enhanced schema
    if not loader.setup_enhanced_schema():
        print("❌ Failed to set up database schema")
        sys.exit(1)
    
    data_dir = "data/generated"
    
    # Load transactions
    transactions_file = os.path.join(data_dir, "transactions.json")
    if os.path.exists(transactions_file):
        loader.load_transactions(transactions_file)
    else:
        print(f"⚠️ Transactions file not found: {transactions_file}")
        print("   Run advanced_data_generator.py first to generate data")
    
    # Load crypto prices
    prices_file = os.path.join(data_dir, "crypto_prices.json")
    if os.path.exists(prices_file):
        loader.load_crypto_prices(prices_file)
    else:
        print(f"⚠️ Crypto prices file not found: {prices_file}")
    
    # Load trading volumes
    trading_file = os.path.join(data_dir, "trading_data.json")
    if os.path.exists(trading_file):
        loader.load_trading_volumes(trading_file)
    else:
        print(f"⚠️ Trading data file not found: {trading_file}")
    
    # Verify loading
    loader.verify_data_loading()
    
    print("\n🎉 Data loading complete!")
    print("✅ ClickHouse is ready for business analytics")
    print("✅ Ready for Metabase BI dashboard integration")

if __name__ == "__main__":
    main()