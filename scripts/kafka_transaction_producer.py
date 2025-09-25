#!/usr/bin/env python3
"""
🚀 Real-time Transaction Streaming Producer
📡 Kafka Producer using Faker for continuous transaction generation

This script generates fake transactions in real-time and streams them to Kafka topics
for processing by downstream consumers and fraud detection systems.

🎯 Features:
- 🔄 Continuous transaction generation with realistic timing
- 📡 Multi-topic Kafka streaming (normal + suspicious transactions)
- 🎛️ Configurable transaction rates and patterns
- 🚨 Real-time fraud pattern injection
- 📊 Monitoring and metrics collection
"""

import json
import time
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import logging
import threading
from dataclasses import dataclass

# 📦 Required dependencies
try:
    from kafka import KafkaProducer
    from faker import Faker
except ImportError as e:
    print("❌ Missing required dependencies!")
    print("Please install: pip install kafka-python faker")
    print(f"Error: {e}")
    exit(1)

# 🎨 Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class StreamingConfig:
    """📋 Configuration for streaming parameters"""
    kafka_bootstrap_servers: str = "localhost:9092"
    normal_topic: str = "transactions-normal"
    suspicious_topic: str = "transactions-suspicious"
    fraud_topic: str = "transactions-fraud-alerts"
    
    # 🎛️ Rate control
    transactions_per_second: float = 2.0
    fraud_injection_rate: float = 0.05  # 5% fraud rate
    burst_probability: float = 0.1      # 10% chance of burst
    
    # 🌍 Geographic distribution
    country_weights: Dict[str, float] = None
    
    def __post_init__(self):
        if self.country_weights is None:
            self.country_weights = {
                'US': 0.25, 'SG': 0.20, 'CN': 0.15, 'UK': 0.12,
                'JP': 0.10, 'KR': 0.08, 'MY': 0.05, 'PH': 0.03, 'VN': 0.02
            }

class RealTimeTransactionStreamer:
    def __init__(self, config: StreamingConfig):
        """🏗️ Initialize the real-time transaction streamer"""
        self.config = config
        self.producer = None
        self.faker_global = Faker(['en_US', 'en_GB', 'zh_CN', 'ko_KR', 'ja_JP'])
        self.faker_thailand = Faker(['th_TH', 'en_US'])
        
        # 📊 Metrics
        self.transactions_sent = 0
        self.fraud_transactions_sent = 0
        self.start_time = datetime.now()
        self.running = False
        
        # 🎭 Account pools (smaller for streaming)
        self.normal_accounts = self._generate_accounts(50, 'normal')
        self.whale_accounts = self._generate_accounts(10, 'whale') 
        self.suspicious_accounts = self._generate_accounts(5, 'suspicious')
        
        # 🔄 Setup Kafka producer
        self._setup_kafka_producer()
    
    def _setup_kafka_producer(self):
        """📡 Initialize Kafka producer with optimized settings"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                
                # 🚀 Performance optimizations
                acks='all',                    # Ensure message durability
                retries=3,                     # Retry failed sends
                batch_size=16384,              # Batch messages for efficiency
                linger_ms=5,                   # Small delay to batch messages
                buffer_memory=33554432,        # 32MB buffer
                compression_type='gzip',       # Compress messages (gzip is widely supported)
                
                # 🔧 Reliability settings
                enable_idempotence=True,       # Prevent duplicate messages
                max_in_flight_requests_per_connection=1,  # Required for idempotence
            )
            logger.info(f"✅ Kafka producer initialized: {self.config.kafka_bootstrap_servers}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            raise
    
    def _generate_accounts(self, count: int, account_type: str) -> list:
        """👥 Generate account pools for different user types"""
        accounts = []
        for _ in range(count):
            if account_type == 'suspicious':
                countries = ['US', 'RU', 'CN']
                preferred_crypto = ['USDT', 'USDC', 'BTC']
            elif account_type == 'whale':
                countries = ['US', 'UK', 'CN', 'SG', 'JP']
                preferred_crypto = list(self._get_crypto_rates().keys())
            else:
                countries = list(self.config.country_weights.keys())
                preferred_crypto = list(self._get_crypto_rates().keys())
            
            account = {
                'user_id': str(uuid.uuid4()),
                'account_type': account_type,
                'email': self.faker_global.email(),
                'country': random.choice(countries),
                'kyc_level': 3 if account_type == 'whale' else random.randint(1, 3),
                'preferred_crypto': random.choice(preferred_crypto)
            }
            accounts.append(account)
        
        return accounts
    
    def _get_crypto_rates(self) -> Dict[str, float]:
        """💰 Get current crypto USD rates"""
        return {
            'BTC': 45000.00, 'ETH': 2800.00, 'USDT': 1.00, 'USDC': 1.00,
            'BNB': 320.00, 'ADA': 0.45, 'DOT': 6.50, 'XRP': 0.62,
            'SOL': 98.50, 'MATIC': 0.85, 'AVAX': 28.50, 'LINK': 14.20,
            'UNI': 6.80, 'LTC': 95.00, 'BCH': 240.00
        }
    
    def _generate_normal_transaction(self) -> Dict[str, Any]:
        """✅ Generate a normal transaction"""
        # Select account based on distribution
        account_pools = [
            (self.normal_accounts, 0.7),
            (self.whale_accounts, 0.3)
        ]
        account_pool = random.choices(
            [pool for pool, _ in account_pools],
            weights=[weight for _, weight in account_pools]
        )[0]
        
        sender = random.choice(account_pool)
        
        # Generate Thai receiver
        receiver = {
            'user_id': str(uuid.uuid4()),
            'email': self.faker_thailand.email(),
            'country': 'TH',
            'exchange': 'Binance_TH',
            'kyc_level': random.choice([2, 3])
        }
        
        # Transaction amounts based on account type
        crypto = sender['preferred_crypto']
        crypto_rates = self._get_crypto_rates()
        
        if sender['account_type'] == 'whale':
            usd_amount = random.uniform(10000, 200000)
        else:
            usd_amount = random.uniform(100, 10000)
        
        # Calculate amounts
        crypto_amount = usd_amount / crypto_rates[crypto]
        thb_amount = usd_amount * 35.50
        
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'sender': sender,
            'receiver': receiver,
            'transfer': {
                'crypto_symbol': crypto,
                'crypto_amount': round(crypto_amount, 8),
                'usd_equivalent': round(usd_amount, 2),
                'thb_equivalent': round(thb_amount, 2),
                'exchange_rate_usd_thb': 35.50
            },
            'fees': {
                'fee_usd': round(usd_amount * 0.001, 4),
                'fee_rate': 0.001,
                'fee_type': 'maker'
            },
            'status': 'pending',
            'compliance': {
                'aml_score': round(random.uniform(0.1, 0.3), 3),
                'risk_level': 'low'
            },
            'stream_metadata': {
                'producer_id': 'faker-stream-producer',
                'generated_at': datetime.now().isoformat(),
                'sequence_id': self.transactions_sent + 1
            }
        }
        
        return transaction
    
    def _generate_fraud_transaction(self) -> Dict[str, Any]:
        """🚨 Generate a suspicious/fraudulent transaction"""
        sender = random.choice(self.suspicious_accounts)
        
        receiver = {
            'user_id': str(uuid.uuid4()),
            'email': self.faker_thailand.email(),
            'country': 'TH',
            'exchange': 'Binance_TH',
            'kyc_level': random.choice([1, 2])
        }
        
        # High-risk patterns
        crypto = random.choice(['USDT', 'USDC', 'BTC'])
        usd_amount = random.uniform(30000, 100000)  # High amount
        crypto_rates = self._get_crypto_rates()
        crypto_amount = usd_amount / crypto_rates[crypto]
        
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'sender': sender,
            'receiver': receiver,
            'transfer': {
                'crypto_symbol': crypto,
                'crypto_amount': round(crypto_amount, 8),
                'usd_equivalent': round(usd_amount, 2),
                'thb_equivalent': round(usd_amount * 35.50, 2),
                'exchange_rate_usd_thb': 35.50
            },
            'fees': {
                'fee_usd': round(usd_amount * 0.0015, 4),
                'fee_rate': 0.0015,
                'fee_type': 'taker'
            },
            'status': 'pending',
            'compliance': {
                'aml_score': round(random.uniform(0.7, 0.95), 3),
                'risk_level': 'high',
                'flags': ['high_amount', 'suspicious_pattern']
            },
            'stream_metadata': {
                'producer_id': 'faker-stream-producer',
                'generated_at': datetime.now().isoformat(),
                'sequence_id': self.transactions_sent + 1,
                'fraud_type': 'high_amount_suspicious'
            }
        }
        
        return transaction
    
    def _send_transaction(self, transaction: Dict[str, Any]):
        """📡 Send transaction to appropriate Kafka topic"""
        try:
            # Determine topic based on risk level
            is_fraud = transaction['compliance']['risk_level'] in ['medium', 'high']
            topic = self.config.suspicious_topic if is_fraud else self.config.normal_topic
            
            # Use sender country as partition key for better distribution
            partition_key = transaction['sender']['country']
            
            # Send to Kafka
            future = self.producer.send(
                topic,
                value=transaction,
                key=partition_key
            )
            
            # Optional: Wait for confirmation (adds latency but ensures delivery)
            # record_metadata = future.get(timeout=10)
            
            # Update metrics
            self.transactions_sent += 1
            if is_fraud:
                self.fraud_transactions_sent += 1
            
            # Log progress every 100 transactions
            if self.transactions_sent % 100 == 0:
                self._log_metrics()
                
        except Exception as e:
            logger.error(f"❌ Failed to send transaction: {e}")
    
    def _log_metrics(self):
        """📊 Log streaming metrics"""
        runtime = datetime.now() - self.start_time
        rate = self.transactions_sent / runtime.total_seconds() if runtime.total_seconds() > 0 else 0
        
        logger.info(f"📊 Streaming metrics:")
        logger.info(f"   💫 Total sent: {self.transactions_sent:,}")
        logger.info(f"   🚨 Fraud sent: {self.fraud_transactions_sent:,}")
        logger.info(f"   ⚡ Rate: {rate:.2f} tx/sec")
        logger.info(f"   ⏱️ Runtime: {runtime}")
    
    def start_streaming(self):
        """🚀 Start the real-time streaming process"""
        logger.info("🚀 Starting real-time transaction streaming...")
        logger.info(f"📊 Target rate: {self.config.transactions_per_second} tx/sec")
        
        self.running = True
        self.start_time = datetime.now()
        
        try:
            while self.running:
                # Generate transaction type based on fraud rate
                is_fraud = random.random() < self.config.fraud_injection_rate
                
                if is_fraud:
                    transaction = self._generate_fraud_transaction()
                else:
                    transaction = self._generate_normal_transaction()
                
                # Send transaction
                self._send_transaction(transaction)
                
                # Rate limiting
                sleep_time = 1.0 / self.config.transactions_per_second
                
                # Add some randomness to avoid predictable patterns
                jitter = random.uniform(-0.1, 0.1) * sleep_time
                actual_sleep = max(0.001, sleep_time + jitter)
                
                time.sleep(actual_sleep)
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping streaming (user interrupt)")
        except Exception as e:
            logger.error(f"❌ Streaming error: {e}")
        finally:
            self.stop_streaming()
    
    def stop_streaming(self):
        """🛑 Stop the streaming process gracefully"""
        logger.info("🛑 Stopping transaction streaming...")
        self.running = False
        
        if self.producer:
            self.producer.flush()  # Ensure all messages are sent
            self.producer.close()  # Close producer
        
        self._log_metrics()
        logger.info("✅ Streaming stopped successfully")

def main():
    """🎯 Main execution function"""
    print("🌊 Real-time Transaction Streaming with Kafka")
    print("=" * 50)
    
    # Configuration
    config = StreamingConfig(
        transactions_per_second=3.0,  # 3 transactions per second
        fraud_injection_rate=0.05,    # 5% fraud rate
    )
    
    try:
        # Initialize streamer
        streamer = RealTimeTransactionStreamer(config)
        
        # Start streaming
        streamer.start_streaming()
        
    except Exception as e:
        logger.error(f"❌ Failed to start streaming: {e}")
        print("\n💡 Make sure Kafka is running:")
        print("   docker-compose up -d kafka zookeeper")

if __name__ == "__main__":
    main()