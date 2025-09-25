#!/usr/bin/env python3
"""
Fake Transaction Generator for Cross-Border Transfers
Binance Global to Binance Thailand

This script generates realistic fake transaction data to support business analytics
and answer key business questions about cross-border transfers.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any

import os

# Add faker dependency
try:
    from faker import Faker
except ImportError:
    print("Please install faker: pip install faker")
    exit(1)

# Initialize faker with different locales
fake_global = Faker(['en_US', 'en_GB', 'zh_CN', 'ko_KR', 'ja_JP'])
fake_thailand = Faker(['th_TH', 'en_US'])

# Cryptocurrency data based on real exchange rates (approximate)
CRYPTO_USD_RATES = {
    'BTC': 45000.00,
    'ETH': 2800.00,
    'USDT': 1.00,
    'USDC': 1.00,
    'BNB': 320.00,
    'ADA': 0.45,
    'DOT': 6.50,
    'XRP': 0.62,
    'SOL': 98.50,
    'MATIC': 0.85,
    'AVAX': 28.50,
    'LINK': 14.20,
    'UNI': 6.80,
    'LTC': 95.00,
    'BCH': 240.00
}

# USD to THB rate (approximate)
USD_TO_THB = 35.50

# Exchange fee structure
EXCHANGE_FEE_RATES = {
    'maker': 0.001,  # 0.1%
    'taker': 0.0015  # 0.15%
}

class TransactionGenerator:
    def __init__(self, seed: int = 42):
        """Initialize the transaction generator with a seed for reproducibility."""
        random.seed(seed)
        Faker.seed(seed)
        
        # Account pools for different user types
        self.whale_accounts = []  # High-volume traders
        self.regular_accounts = []  # Normal users
        self.suspicious_accounts = []  # For fraud detection scenarios
        
        self._generate_account_pools()
    
    def _generate_account_pools(self):
        """Generate different types of user accounts."""
        # Generate whale accounts (10 accounts)
        for _ in range(10):
            account = {
                'user_id': fake_global.uuid4(),
                'account_type': 'whale',
                'email': fake_global.email(),
                'country': random.choice(['US', 'UK', 'CN', 'KR', 'JP', 'SG']),
                'registration_date': fake_global.date_between(start_date='-3y', end_date='-1y').isoformat(),
                'kyc_level': 3,
                'preferred_crypto': random.choice(list(CRYPTO_USD_RATES.keys()))
            }
            self.whale_accounts.append(account)
        
        # Generate regular accounts (100 accounts)
        for _ in range(100):
            account = {
                'user_id': fake_global.uuid4(),
                'account_type': 'regular',
                'email': fake_global.email(),
                'country': random.choice(['US', 'UK', 'CN', 'KR', 'JP', 'SG', 'MY', 'VN', 'PH']),
                'registration_date': fake_global.date_between(start_date='-2y', end_date='-1m').isoformat(),
                'kyc_level': random.choice([1, 2, 3]),
                'preferred_crypto': random.choice(list(CRYPTO_USD_RATES.keys()))
            }
            self.regular_accounts.append(account)
        
        # Generate suspicious accounts for fraud detection (5 accounts)
        for _ in range(5):
            account = {
                'user_id': fake_global.uuid4(),
                'account_type': 'suspicious',
                'email': fake_global.email(),
                'country': random.choice(['US', 'RU', 'CN']),
                'registration_date': fake_global.date_between(start_date='-3m', end_date='-1d').isoformat(),
                'kyc_level': 1,
                'preferred_crypto': random.choice(['USDT', 'USDC', 'BTC'])
            }
            self.suspicious_accounts.append(account)
    
    def _generate_thai_receiver(self) -> Dict[str, Any]:
        """Generate a Thai receiver account."""
        return {
            'user_id': fake_thailand.uuid4(),
            'email': fake_thailand.email(),
            'country': 'TH',
            'exchange': 'Binance_TH',
            'kyc_level': random.choice([2, 3]),
            'registration_date': fake_thailand.date_between(start_date='-2y', end_date='-1m').isoformat()
        }
    
    def _calculate_amounts(self, crypto: str, base_usd_amount: float) -> Dict[str, float]:
        """Calculate crypto amount and equivalent values."""
        crypto_rate = CRYPTO_USD_RATES[crypto]
        crypto_amount = base_usd_amount / crypto_rate
        thb_equivalent = base_usd_amount * USD_TO_THB
        
        # Round to appropriate decimal places
        if crypto in ['BTC', 'ETH']:
            crypto_amount = round(crypto_amount, 8)
        elif crypto in ['USDT', 'USDC']:
            crypto_amount = round(crypto_amount, 2)
        else:
            crypto_amount = round(crypto_amount, 6)
        
        return {
            'crypto_amount': crypto_amount,
            'usd_equivalent': round(base_usd_amount, 2),
            'thb_equivalent': round(thb_equivalent, 2)
        }
    
    def _calculate_fees(self, usd_amount: float, is_maker: bool = True) -> Dict[str, float]:
        """Calculate exchange fees."""
        fee_rate = EXCHANGE_FEE_RATES['maker'] if is_maker else EXCHANGE_FEE_RATES['taker']
        fee_usd = usd_amount * fee_rate
        fee_thb = fee_usd * USD_TO_THB
        
        return {
            'fee_usd': round(fee_usd, 4),
            'fee_thb': round(fee_thb, 4),
            'fee_rate': fee_rate,
            'fee_type': 'maker' if is_maker else 'taker'
        }
    
    def generate_normal_transaction(self, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate a normal cross-border transaction."""
        if timestamp is None:
            timestamp = fake_global.date_time_between(start_date='-1y', end_date='now')
        
        # Select sender (weighted towards regular accounts)
        account_pool = random.choices(
            [self.regular_accounts, self.whale_accounts],
            weights=[0.8, 0.2]
        )[0]
        sender = random.choice(account_pool)
        
        # Generate receiver
        receiver = self._generate_thai_receiver()
        
        # Select crypto and amount based on account type
        crypto = sender['preferred_crypto']
        if sender['account_type'] == 'whale':
            base_usd = random.uniform(50000, 500000)  # $50K - $500K
        else:
            base_usd = random.uniform(100, 50000)     # $100 - $50K
        
        amounts = self._calculate_amounts(crypto, base_usd)
        fees = self._calculate_fees(base_usd, random.choice([True, False]))
        
        # Generate transaction ID and other metadata
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'timestamp': timestamp.isoformat(),
            'sender': {
                'user_id': sender['user_id'],
                'email': sender['email'],
                'country': sender['country'],
                'exchange': 'Binance_Global',
                'account_type': sender['account_type'],
                'kyc_level': sender['kyc_level']
            },
            'receiver': receiver,
            'transfer': {
                'crypto_symbol': crypto,
                'crypto_amount': amounts['crypto_amount'],
                'usd_equivalent': amounts['usd_equivalent'],
                'thb_equivalent': amounts['thb_equivalent'],
                'exchange_rate_usd_thb': USD_TO_THB,
                'crypto_rate_usd': CRYPTO_USD_RATES[crypto]
            },
            'fees': fees,
            'network': {
                'network_type': self._get_network_type(crypto),
                'confirmation_blocks': random.randint(3, 12),
                'network_fee_usd': round(random.uniform(0.5, 15.0), 4)
            },
            'status': 'completed',
            'processing_time_minutes': random.randint(5, 45),
            'compliance': {
                'aml_score': round(random.uniform(0.1, 0.3), 3),
                'sanctions_check': 'passed',
                'risk_level': 'low'
            }
        }
        
        return transaction
    
    def generate_fraud_transaction(self, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate a suspicious transaction for fraud detection scenarios."""
        if timestamp is None:
            timestamp = fake_global.date_time_between(start_date='-30d', end_date='now')
        
        sender = random.choice(self.suspicious_accounts)
        receiver = self._generate_thai_receiver()
        
        # Fraud patterns: high amounts, stable coins, rapid succession
        crypto = random.choice(['USDT', 'USDC', 'BTC'])
        base_usd = random.uniform(30000, 100000)  # Above $30K threshold
        
        amounts = self._calculate_amounts(crypto, base_usd)
        fees = self._calculate_fees(base_usd, False)  # Usually taker
        
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'timestamp': timestamp.isoformat(),
            'sender': {
                'user_id': sender['user_id'],
                'email': sender['email'],
                'country': sender['country'],
                'exchange': 'Binance_Global',
                'account_type': sender['account_type'],
                'kyc_level': sender['kyc_level']
            },
            'receiver': receiver,
            'transfer': {
                'crypto_symbol': crypto,
                'crypto_amount': amounts['crypto_amount'],
                'usd_equivalent': amounts['usd_equivalent'],
                'thb_equivalent': amounts['thb_equivalent'],
                'exchange_rate_usd_thb': USD_TO_THB,
                'crypto_rate_usd': CRYPTO_USD_RATES[crypto]
            },
            'fees': fees,
            'network': {
                'network_type': self._get_network_type(crypto),
                'confirmation_blocks': random.randint(1, 6),  # Faster confirmations
                'network_fee_usd': round(random.uniform(0.5, 8.0), 4)
            },
            'status': 'completed',
            'processing_time_minutes': random.randint(2, 15),  # Faster processing
            'compliance': {
                'aml_score': round(random.uniform(0.6, 0.9), 3),  # Higher risk
                'sanctions_check': random.choice(['passed', 'flagged']),
                'risk_level': random.choice(['medium', 'high'])
            }
        }
        
        return transaction
    
    def _get_network_type(self, crypto: str) -> str:
        """Get network type for cryptocurrency."""
        network_map = {
            'BTC': 'Bitcoin',
            'ETH': 'Ethereum',
            'USDT': random.choice(['Ethereum', 'Tron', 'BSC']),
            'USDC': random.choice(['Ethereum', 'Polygon']),
            'BNB': 'BSC',
            'ADA': 'Cardano',
            'DOT': 'Polkadot',
            'XRP': 'Ripple',
            'SOL': 'Solana',
            'MATIC': 'Polygon',
            'AVAX': 'Avalanche',
            'LINK': 'Ethereum',
            'UNI': 'Ethereum',
            'LTC': 'Litecoin',
            'BCH': 'Bitcoin Cash'
        }
        return network_map.get(crypto, 'Ethereum')
    
    def generate_fraud_pattern_transactions(self, 
                                          sender_id: str, 
                                          num_receivers: int = 8, 
                                          time_window_minutes: int = 1) -> List[Dict[str, Any]]:
        """Generate fraud pattern: one sender to multiple receivers in short time window."""
        base_time = fake_global.date_time_between(start_date='-7d', end_date='now')
        transactions = []
        
        # Find the suspicious sender
        sender = None
        for account in self.suspicious_accounts:
            if account['user_id'] == sender_id:
                sender = account
                break
        
        if not sender:
            sender = random.choice(self.suspicious_accounts)
        
        total_amount = 0
        for i in range(num_receivers):
            # Distribute transactions within the time window
            time_offset = random.uniform(0, time_window_minutes * 60)  # seconds
            tx_time = base_time + timedelta(seconds=time_offset)
            
            receiver = self._generate_thai_receiver()
            
            # Amounts that sum to over $10,000 to meet fraud criteria
            base_usd = random.uniform(1500, 3000)
            total_amount += base_usd
            
            crypto = random.choice(['USDT', 'USDC'])
            amounts = self._calculate_amounts(crypto, base_usd)
            fees = self._calculate_fees(base_usd, False)
            
            transaction = {
                'transaction_id': str(uuid.uuid4()),
                'timestamp': tx_time.isoformat(),
                'sender': {
                    'user_id': sender['user_id'],
                    'email': sender['email'],
                    'country': sender['country'],
                    'exchange': 'Binance_Global',
                    'account_type': sender['account_type'],
                    'kyc_level': sender['kyc_level']
                },
                'receiver': receiver,
                'transfer': {
                    'crypto_symbol': crypto,
                    'crypto_amount': amounts['crypto_amount'],
                    'usd_equivalent': amounts['usd_equivalent'],
                    'thb_equivalent': amounts['thb_equivalent'],
                    'exchange_rate_usd_thb': USD_TO_THB,
                    'crypto_rate_usd': CRYPTO_USD_RATES[crypto]
                },
                'fees': fees,
                'network': {
                    'network_type': self._get_network_type(crypto),
                    'confirmation_blocks': random.randint(1, 3),
                    'network_fee_usd': round(random.uniform(0.5, 3.0), 4)
                },
                'status': 'completed',
                'processing_time_minutes': random.randint(1, 8),
                'compliance': {
                    'aml_score': round(random.uniform(0.7, 0.95), 3),
                    'sanctions_check': 'flagged',
                    'risk_level': 'high'
                },
                'fraud_pattern': {
                    'pattern_type': 'multiple_receivers_short_window',
                    'total_recipients_in_window': num_receivers,
                    'time_window_minutes': time_window_minutes,
                    'pattern_total_usd': round(total_amount if i == num_receivers - 1 else 0, 2)
                }
            }
            
            transactions.append(transaction)
        
        return transactions
    
    def generate_dataset(self, 
                        num_normal: int = 10000, 
                        num_fraud: int = 500,
                        num_fraud_patterns: int = 20) -> List[Dict[str, Any]]:
        """Generate complete dataset with normal and fraudulent transactions."""
        transactions = []
        
        print(f"Generating {num_normal} normal transactions...")
        for _ in range(num_normal):
            transactions.append(self.generate_normal_transaction())
        
        print(f"Generating {num_fraud} individual fraud transactions...")
        for _ in range(num_fraud):
            transactions.append(self.generate_fraud_transaction())
        
        print(f"Generating {num_fraud_patterns} fraud pattern groups...")
        for _ in range(num_fraud_patterns):
            # Pick a random suspicious account
            sender_id = random.choice(self.suspicious_accounts)['user_id']
            pattern_transactions = self.generate_fraud_pattern_transactions(sender_id)
            transactions.extend(pattern_transactions)
        
        # Sort by timestamp
        transactions.sort(key=lambda x: x['timestamp'])
        
        return transactions
    
    def save_to_files(self, transactions: List[Dict[str, Any]], base_path: str):
        """Save transactions to JSON file only."""
        os.makedirs(base_path, exist_ok=True)
        
        # Save as JSON
        json_path = os.path.join(base_path, 'fake_transactions.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(transactions, f, indent=2, ensure_ascii=False)
        
        # Generate summary statistics
        self._generate_summary_stats(transactions, base_path)
        
        print(f"Saved {len(transactions)} transactions to:")
        print(f"  JSON: {json_path}")
    

    
    def _generate_summary_stats(self, transactions: List[Dict[str, Any]], base_path: str):
        """Generate summary statistics."""
        total_volume_usd = sum(tx['transfer']['usd_equivalent'] for tx in transactions)
        total_volume_thb = sum(tx['transfer']['thb_equivalent'] for tx in transactions)
        
        crypto_breakdown = {}
        country_breakdown = {}
        fraud_count = 0
        
        for tx in transactions:
            crypto = tx['transfer']['crypto_symbol']
            country = tx['sender']['country']
            
            crypto_breakdown[crypto] = crypto_breakdown.get(crypto, 0) + 1
            country_breakdown[country] = country_breakdown.get(country, 0) + 1
            
            if tx['compliance']['risk_level'] in ['medium', 'high']:
                fraud_count += 1
        
        stats = {
            'total_transactions': len(transactions),
            'total_volume_usd': round(total_volume_usd, 2),
            'total_volume_thb': round(total_volume_thb, 2),
            'average_transaction_usd': round(total_volume_usd / len(transactions), 2),
            'suspicious_transactions': fraud_count,
            'fraud_rate': round(fraud_count / len(transactions) * 100, 2),
            'crypto_distribution': crypto_breakdown,
            'country_distribution': country_breakdown,
            'date_range': {
                'earliest': min(tx['timestamp'] for tx in transactions),
                'latest': max(tx['timestamp'] for tx in transactions)
            }
        }
        
        stats_path = os.path.join(base_path, 'transaction_summary.json')
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        print(f"Summary statistics saved to: {stats_path}")


def main():
    """Main execution function."""
    print("🚀 Generating fake cross-border transaction data...")
    print("   Binance Global → Binance Thailand")
    print("=" * 50)
    
    # Initialize generator
    generator = TransactionGenerator(seed=42)
    
    # Generate dataset
    transactions = generator.generate_dataset(
        num_normal=8000,      # Normal transactions
        num_fraud=300,        # Individual fraud transactions
        num_fraud_patterns=15 # Fraud pattern groups
    )
    
    print(f"\n📊 Generated {len(transactions)} total transactions")
    
    # Save to files in correct project path
    base_path = "data/raw/fake_transactions"
    generator.save_to_files(transactions, base_path)
    
    print("\n✅ Transaction generation complete!")
    print("\nKey Business Questions this data can answer:")
    print("1. Cross-border Transfer Value (USD/THB equivalent)")
    print("2. Top Accounts by Transfer Volume")
    print("3. Top Senders from Global to TH")
    print("4. Top Receivers from Global to TH")
    print("5. Fraud Detection (>$30K + >$10K to >5 accounts in 1 minute)")
    
    print("\n📁 Generated files:")
    print(f"   - {base_path}/fake_transactions.json")
    print(f"   - {base_path}/transaction_summary.json")


if __name__ == "__main__":
    main()