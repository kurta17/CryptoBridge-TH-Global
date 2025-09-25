#!/usr/bin/env python3
"""
🎭 Advanced Data Generator for CryptoBridge Analytics
Generates fake transaction data and integrates real crypto API data
for comprehensive business intelligence requirements.
"""

import json
import random
import uuid
import asyncio
import aiohttp
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional
import time
import os
import sys

try:
    from faker import Faker
    import requests
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install faker requests pandas numpy")
    sys.exit(1)

# Initialize Faker for different regions
fake_global = Faker(["en_US", "en_GB", "zh_CN", "ko_KR", "ja_JP"])
fake_thailand = Faker(["th_TH", "en_US"])


class CryptoBridgeDataGenerator:
    def __init__(self, output_dir: str = "../data/generated"):
        """Initialize the advanced data generator"""
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        # Binance API configuration
        self.binance_base_url = "https://api.binance.com/api/v3"
        self.binance_th_url = "https://api.binance.th/api/v3"

        # Account pools for business scenarios
        self.whale_accounts = []  # High-volume traders (>$100K transfers)
        self.regular_accounts = []  # Normal users ($100-$50K)
        self.suspicious_accounts = []  # For fraud detection
        self.thai_receivers = []  # Thailand receivers

        # Real-time crypto prices
        self.crypto_prices = {}
        self.price_history = {}

        # Business scenario configurations
        self.fraud_scenarios = self._setup_fraud_scenarios()

        self._initialize_accounts()
        print("🚀 CryptoBridge Data Generator initialized!")

    def _setup_fraud_scenarios(self) -> List[Dict]:
        """Configure fraud detection scenarios"""
        return [
            {
                "name": "High Frequency Large Transfers",
                "pattern": "payments > $30K AND total sends > $10K to >5 accounts in 1 minute",
                "min_amount": 30000,
                "total_threshold": 10000,
                "min_recipients": 5,
                "time_window_minutes": 1,
            },
            {
                "name": "Rapid Fire Transfers",
                "pattern": "Multiple small transfers to avoid detection",
                "min_amount": 9000,
                "total_threshold": 50000,
                "min_recipients": 8,
                "time_window_minutes": 5,
            },
            {
                "name": "Round Amount Laundering",
                "pattern": "Exact round amounts indicating potential laundering",
                "amounts": [10000, 25000, 50000, 100000],
                "frequency": "high",
            },
        ]

    def _initialize_accounts(self):
        """Generate different types of user accounts for realistic scenarios"""
        print("👥 Generating account pools...")

        # Generate whale accounts (20 accounts) - for top volume analysis
        for i in range(20):
            account = {
                "user_id": f"WHALE_{fake_global.uuid4()}",
                "account_type": "whale",
                "email": fake_global.email(),
                "name": fake_global.name(),
                "country": random.choice(
                    ["US", "UK", "CN", "KR", "JP", "SG", "CA", "DE"]
                ),
                "registration_date": fake_global.date_between(
                    start_date="-3y", end_date="-6m"
                ),
                "kyc_level": 3,
                "preferred_cryptos": random.sample(
                    ["BTC", "ETH", "USDT", "USDC", "BNB"], 3
                ),
                "monthly_volume_usd": random.uniform(500000, 5000000),  # $500K - $5M
                "risk_profile": (
                    "low" if i < 15 else "medium"
                ),  # Most whales are low risk
            }
            self.whale_accounts.append(account)

        # Generate regular accounts (200 accounts)
        for i in range(200):
            account = {
                "user_id": f"REG_{fake_global.uuid4()}",
                "account_type": "regular",
                "email": fake_global.email(),
                "name": fake_global.name(),
                "country": random.choice(
                    ["US", "UK", "CN", "KR", "JP", "SG", "MY", "VN", "PH", "ID"]
                ),
                "registration_date": fake_global.date_between(
                    start_date="-2y", end_date="-1w"
                ),
                "kyc_level": random.choices([1, 2, 3], weights=[0.2, 0.5, 0.3])[0],
                "preferred_cryptos": random.sample(
                    ["BTC", "ETH", "USDT", "USDC", "BNB", "ADA", "DOT"], 2
                ),
                "monthly_volume_usd": random.uniform(1000, 100000),  # $1K - $100K
                "risk_profile": random.choices(["low", "medium"], weights=[0.8, 0.2])[
                    0
                ],
            }
            self.regular_accounts.append(account)

        # Generate suspicious accounts (10 accounts) - for fraud detection
        for i in range(10):
            account = {
                "user_id": f"SUSP_{fake_global.uuid4()}",
                "account_type": "suspicious",
                "email": fake_global.email(),
                "name": fake_global.name(),
                "country": random.choice(
                    ["US", "RU", "CN", "VE", "IR"]
                ),  # Higher risk countries
                "registration_date": fake_global.date_between(
                    start_date="-6m", end_date="-1d"
                ),
                "kyc_level": 1,  # Low KYC level
                "preferred_cryptos": [
                    "USDT",
                    "USDC",
                    "BTC",
                ],  # Privacy coins preference
                "monthly_volume_usd": random.uniform(
                    100000, 1000000
                ),  # High volume, low KYC
                "risk_profile": "high",
                "fraud_scenario": random.choice(self.fraud_scenarios),
            }
            self.suspicious_accounts.append(account)

        # Generate Thai receivers (150 accounts)
        for i in range(150):
            receiver = {
                "user_id": f"TH_{fake_thailand.uuid4()}",
                "email": fake_thailand.email(),
                "name": fake_thailand.name(),
                "country": "TH",
                "exchange": "Binance_TH",
                "kyc_level": random.choices([2, 3], weights=[0.3, 0.7])[0],
                "registration_date": fake_thailand.date_between(
                    start_date="-2y", end_date="-1w"
                ),
                "preferred_cryptos": random.sample(
                    ["BTC", "ETH", "USDT", "USDC", "BNB"], 2
                ),
                "account_type": random.choices(
                    ["regular", "business"], weights=[0.7, 0.3]
                )[0],
            }
            self.thai_receivers.append(receiver)

        print(
            f"✅ Generated {len(self.whale_accounts)} whale, {len(self.regular_accounts)} regular, "
            f"{len(self.suspicious_accounts)} suspicious accounts, {len(self.thai_receivers)} Thai receivers"
        )

    async def fetch_real_crypto_data(self) -> Dict[str, Any]:
        """Fetch real cryptocurrency data from Binance APIs"""
        print("📡 Fetching real crypto data from APIs...")

        crypto_data = {
            "prices": {},
            "trading_volumes": {},
            "price_history": {},
            "thb_pairs": {},
        }

        # THB trading pairs to fetch
        thb_pairs = ["BTCTHB", "ETHTHB", "USDTTHB", "BNBTHB", "ADATHB", "DOTTHB"]

        async with aiohttp.ClientSession() as session:
            try:
                # Fetch current prices for major pairs
                async with session.get(
                    f"{self.binance_base_url}/ticker/price"
                ) as response:
                    if response.status == 200:
                        price_data = await response.json()
                        for item in price_data:
                            symbol = item["symbol"]
                            price = float(item["price"])
                            crypto_data["prices"][symbol] = price

                # Fetch Thailand-specific pairs
                try:
                    async with session.get(
                        f"{self.binance_th_url}/ticker/price"
                    ) as response:
                        if response.status == 200:
                            th_price_data = await response.json()
                            for item in th_price_data:
                                symbol = item["symbol"]
                                if symbol in thb_pairs:
                                    crypto_data["thb_pairs"][symbol] = float(
                                        item["price"]
                                    )
                except:
                    print("⚠️ Binance TH API not accessible, using mock data")
                    # Mock THB pair data
                    crypto_data["thb_pairs"] = {
                        "BTCTHB": 1580000.00,  # ~$45K * 35.1
                        "ETHTHB": 98280.00,  # ~$2.8K * 35.1
                        "USDTTHB": 35.10,
                        "BNBTHB": 11232.00,  # ~$320 * 35.1
                        "ADATHB": 15.80,  # ~$0.45 * 35.1
                        "DOTTHB": 228.15,  # ~$6.5 * 35.1
                    }

                # Fetch 24h trading statistics
                async with session.get(
                    f"{self.binance_base_url}/ticker/24hr"
                ) as response:
                    if response.status == 200:
                        stats_data = await response.json()
                        for item in stats_data[:50]:  # Top 50 pairs
                            symbol = item["symbol"]
                            crypto_data["trading_volumes"][symbol] = {
                                "volume": float(item["volume"]),
                                "quote_volume": float(item["quoteVolume"]),
                                "count": int(item["count"]),
                                "price_change": float(item["priceChangePercent"]),
                            }

                print(
                    f"✅ Fetched {len(crypto_data['prices'])} price points, "
                    f"{len(crypto_data['thb_pairs'])} THB pairs, "
                    f"{len(crypto_data['trading_volumes'])} trading stats"
                )

            except Exception as e:
                print(f"⚠️ API fetch error: {e}. Using fallback data...")
                # Fallback crypto prices
                crypto_data["prices"] = {
                    "BTCUSDT": 45000.00,
                    "ETHUSDT": 2800.00,
                    "BNBUSDT": 320.00,
                    "ADAUSDT": 0.45,
                    "DOTUSDT": 6.50,
                    "USDTUSDT": 1.00,
                }

        self.crypto_prices = crypto_data
        return crypto_data

    def generate_cross_border_transactions(
        self,
        num_transactions: int = 10000,
        days_back: int = 30,
        fraud_percentage: float = 0.05,
    ) -> List[Dict[str, Any]]:
        """Generate cross-border transactions for business analysis"""
        print(f"💸 Generating {num_transactions} cross-border transactions...")

        transactions = []
        start_date = datetime.now() - timedelta(days=days_back)
        end_date = datetime.now()

        # Calculate fraud transaction count
        fraud_count = int(num_transactions * fraud_percentage)
        normal_count = num_transactions - fraud_count

        # Generate normal transactions
        for i in range(normal_count):
            # Weighted selection of sender types
            sender_type = random.choices(["whale", "regular"], weights=[0.2, 0.8])[0]

            if sender_type == "whale":
                sender = random.choice(self.whale_accounts)
                base_amount_usd = random.uniform(50000, 500000)  # $50K-$500K
            else:
                sender = random.choice(self.regular_accounts)
                base_amount_usd = random.uniform(100, 50000)  # $100-$50K

            receiver = random.choice(self.thai_receivers)
            crypto = random.choice(sender["preferred_cryptos"])

            # Generate timestamp with realistic patterns (more activity during business hours)
            timestamp = self._generate_business_timestamp(start_date, end_date)

            transaction = self._create_transaction(
                sender, receiver, crypto, base_amount_usd, timestamp
            )
            transactions.append(transaction)

            if i % 1000 == 0:
                print(f"  📊 Generated {i}/{normal_count} normal transactions")

        # Generate fraud transactions
        fraud_transactions = self._generate_fraud_transactions(
            fraud_count, start_date, end_date
        )
        transactions.extend(fraud_transactions)

        # Sort by timestamp
        transactions.sort(key=lambda x: x["timestamp"])

        print(
            f"✅ Generated {len(transactions)} total transactions ({fraud_count} fraudulent)"
        )
        return transactions

    def _generate_business_timestamp(
        self, start_date: datetime, end_date: datetime
    ) -> datetime:
        """Generate realistic business hours timestamp with patterns"""
        # More transactions during business hours and weekdays
        base_timestamp = fake_global.date_time_between(start_date, end_date)

        # Adjust for business patterns
        if base_timestamp.weekday() >= 5:  # Weekend
            if random.random() < 0.3:  # 30% less activity
                base_timestamp += timedelta(days=random.choice([-1, 1, 2]))

        # Peak hours: 9 AM - 5 PM UTC (business hours across multiple timezones)
        if 9 <= base_timestamp.hour <= 17:
            # 60% of transactions happen during business hours
            if random.random() < 0.6:
                return base_timestamp

        return base_timestamp

    def _create_transaction(
        self,
        sender: Dict,
        receiver: Dict,
        crypto: str,
        amount_usd: float,
        timestamp: datetime,
        is_suspicious: bool = False,
    ) -> Dict[str, Any]:
        """Create a standardized transaction record"""

        # Get crypto price (use cached prices or fallback)
        crypto_price_usd = self.crypto_prices.get("prices", {}).get(
            f"{crypto}USDT",
            {"BTC": 45000, "ETH": 2800, "USDT": 1, "USDC": 1, "BNB": 320}.get(
                crypto, 100
            ),
        )

        # Calculate amounts
        crypto_amount = amount_usd / crypto_price_usd
        thb_amount = amount_usd * 35.1  # USD to THB rate

        # Calculate risk score
        risk_score = self._calculate_risk_score(
            sender, receiver, amount_usd, is_suspicious
        )

        return {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": timestamp.isoformat(),
            "from_exchange": "Binance_Global",
            "to_exchange": "Binance_TH",
            "amount": round(crypto_amount, 8),
            "currency": crypto,
            "is_suspicious": 1 if is_suspicious or risk_score > 70 else 0,
            "risk_score": risk_score,
            "country": sender["country"],
            "account_type": sender["account_type"],
            "sender_user_id": sender["user_id"],
            "receiver_user_id": receiver["user_id"],
            "crypto_currency": crypto,
            "fiat_amount": round(amount_usd, 2),
            "exchange_rate": round(crypto_price_usd, 2),
            "transaction_type": "cross_border_transfer",
            "processing_time_ms": random.randint(100, 5000),
            "sender_country": sender["country"],
            "receiver_country": receiver["country"],
            "sender_kyc_level": sender["kyc_level"],
            "receiver_kyc_level": receiver["kyc_level"],
            "thb_equivalent": round(thb_amount, 2),
            "fee_usd": round(amount_usd * 0.001, 4),  # 0.1% fee
            "network_confirmations": random.randint(3, 12),
        }

    def _generate_fraud_transactions(
        self, count: int, start_date: datetime, end_date: datetime
    ) -> List[Dict]:
        """Generate fraud pattern transactions for detection testing"""
        print(f"🚨 Generating {count} fraudulent transactions...")

        fraud_transactions = []

        for i in range(count):
            sender = random.choice(self.suspicious_accounts)
            scenario = sender["fraud_scenario"]

            if scenario["name"] == "High Frequency Large Transfers":
                # Generate burst of large transactions within 1 minute
                base_timestamp = fake_global.date_time_between(start_date, end_date)

                # Create multiple large transfers to different recipients within 1 minute
                num_transfers = random.randint(5, 10)
                for j in range(num_transfers):
                    timestamp = base_timestamp + timedelta(
                        seconds=random.randint(0, 60)
                    )
                    receiver = random.choice(self.thai_receivers)
                    amount_usd = random.uniform(30000, 100000)  # > $30K threshold

                    transaction = self._create_transaction(
                        sender, receiver, "USDT", amount_usd, timestamp, True
                    )
                    fraud_transactions.append(transaction)

            elif scenario["name"] == "Rapid Fire Transfers":
                # Multiple smaller amounts to avoid single transaction limits
                base_timestamp = fake_global.date_time_between(start_date, end_date)

                num_transfers = random.randint(8, 15)
                for j in range(num_transfers):
                    timestamp = base_timestamp + timedelta(minutes=random.randint(0, 5))
                    receiver = random.choice(self.thai_receivers)
                    amount_usd = random.uniform(
                        8000, 12000
                    )  # Just under typical limits

                    transaction = self._create_transaction(
                        sender, receiver, "USDT", amount_usd, timestamp, True
                    )
                    fraud_transactions.append(transaction)

            if i % 10 == 0:
                print(f"  🚨 Generated {i}/{count} fraud scenarios")

        return fraud_transactions

    def _calculate_risk_score(
        self, sender: Dict, receiver: Dict, amount_usd: float, is_suspicious: bool
    ) -> int:
        """Calculate risk score based on multiple factors"""
        risk_score = 0

        # Base risk from account type
        if sender["account_type"] == "suspicious":
            risk_score += 40
        elif sender["account_type"] == "whale":
            risk_score += 10

        # Amount-based risk
        if amount_usd > 100000:
            risk_score += 30
        elif amount_usd > 50000:
            risk_score += 20
        elif amount_usd > 30000:
            risk_score += 15

        # KYC level risk
        if sender["kyc_level"] == 1:
            risk_score += 20
        elif sender["kyc_level"] == 2:
            risk_score += 10

        # Country-based risk
        high_risk_countries = ["RU", "CN", "VE", "IR", "KP"]
        if sender["country"] in high_risk_countries:
            risk_score += 25

        # Time-based patterns (rapid transactions would be detected in real-time)
        if is_suspicious:
            risk_score += 30

        # Random factor for realistic distribution
        risk_score += random.randint(-5, 15)

        return max(0, min(100, risk_score))  # Clamp between 0-100

    async def generate_complete_dataset(self, num_transactions: int = 50000):
        """Generate complete dataset for all business requirements"""
        print("🏗️ Generating complete CryptoBridge dataset...")

        # Step 1: Fetch real crypto data
        await self.fetch_real_crypto_data()

        # Step 2: Generate cross-border transactions (FAKE DATA requirements)
        transactions = self.generate_cross_border_transactions(num_transactions)

        # Step 3: Generate trading data (REAL DATA requirements)
        trading_data = await self.generate_trading_data()

        # Step 4: Save all data
        await self.save_datasets(transactions, trading_data)

        return {
            "transactions": transactions,
            "trading_data": trading_data,
            "crypto_prices": self.crypto_prices,
        }

    async def generate_trading_data(self) -> Dict[str, Any]:
        """Generate trading transaction data for comparison analysis"""
        print("📈 Generating trading data for Binance Global vs TH comparison...")

        trading_data = {
            "global_trades": [],
            "th_trades": [],
            "price_history": {},
            "volume_comparison": {},
        }

        # Use real data from APIs when available
        if self.crypto_prices.get("trading_volumes"):
            for symbol, stats in self.crypto_prices["trading_volumes"].items():
                # Simulate daily/weekly breakdown
                trading_data["volume_comparison"][symbol] = {
                    "global_daily_trades": stats["count"],
                    "global_daily_volume": stats["volume"],
                    "th_daily_trades": int(
                        stats["count"] * 0.1
                    ),  # TH is ~10% of global
                    "th_daily_volume": stats["volume"] * 0.1,
                    "price_change_24h": stats["price_change"],
                }

        return trading_data

    async def save_datasets(self, transactions: List[Dict], trading_data: Dict):
        """Save all generated datasets"""
        print("💾 Saving datasets...")

        # Save transactions for ClickHouse
        transactions_file = os.path.join(self.output_dir, "transactions.json")
        with open(transactions_file, "w") as f:
            json.dump(transactions, f, indent=2)

        # Save trading data
        trading_file = os.path.join(self.output_dir, "trading_data.json")
        with open(trading_file, "w") as f:
            json.dump(trading_data, f, indent=2)

        # Save crypto prices
        prices_file = os.path.join(self.output_dir, "crypto_prices.json")
        with open(prices_file, "w") as f:
            json.dump(self.crypto_prices, f, indent=2)

        # Generate business analysis summaries
        await self.generate_business_summaries(transactions, trading_data)

        print(f"✅ Datasets saved to {self.output_dir}/")
        print(f"   📄 transactions.json: {len(transactions)} records")
        print(f"   📄 trading_data.json: {len(trading_data)} datasets")
        print(
            f"   📄 crypto_prices.json: {len(self.crypto_prices.get('prices', {}))} prices"
        )

    async def generate_business_summaries(
        self, transactions: List[Dict], trading_data: Dict
    ):
        """Generate business summaries for key questions"""
        print("📊 Generating business analysis summaries...")

        # Convert to DataFrame for analysis
        df = pd.DataFrame(transactions)

        summaries = {
            "cross_border_volume": {
                "total_usd": df["fiat_amount"].sum(),
                "total_thb": df["thb_equivalent"].sum(),
                "transaction_count": len(df),
                "average_amount_usd": df["fiat_amount"].mean(),
                "top_crypto_currencies": df["crypto_currency"]
                .value_counts()
                .head(5)
                .to_dict(),
                "daily_volumes": df.groupby(df["timestamp"].str[:10])["fiat_amount"]
                .sum()
                .to_dict(),
            },
            "top_senders": df.groupby("sender_user_id")
            .agg({"fiat_amount": "sum", "transaction_id": "count"})
            .sort_values("fiat_amount", ascending=False)
            .head(10)
            .to_dict(),
            "top_receivers": df.groupby("receiver_user_id")
            .agg({"fiat_amount": "sum", "transaction_id": "count"})
            .sort_values("fiat_amount", ascending=False)
            .head(10)
            .to_dict(),
            "fraud_analysis": {
                "total_suspicious": len(df[df["is_suspicious"] == 1]),
                "suspicious_volume_usd": df[df["is_suspicious"] == 1][
                    "fiat_amount"
                ].sum(),
                "high_risk_transactions": len(df[df["risk_score"] > 70]),
                "fraud_percentage": len(df[df["is_suspicious"] == 1]) / len(df) * 100,
            },
        }

        # Save business summaries
        summaries_file = os.path.join(self.output_dir, "business_summaries.json")
        with open(summaries_file, "w") as f:
            json.dump(summaries, f, indent=2, default=str)

        print("✅ Business summaries generated")

    def print_generation_report(self, data: Dict):
        """Print comprehensive generation report"""
        print("\n" + "=" * 60)
        print("🎯 CRYPTOBRIDGE DATA GENERATION REPORT")
        print("=" * 60)

        transactions = data["transactions"]
        trading_data = data["trading_data"]

        print(f"📊 FAKE DATA GENERATED (For Business Questions 1,2,5,6,7):")
        print(f"   • Total Transactions: {len(transactions):,}")
        print(
            f"   • Cross-border Volume: ${sum(t['fiat_amount'] for t in transactions):,.2f}"
        )
        print(
            f"   • Suspicious Transactions: {sum(1 for t in transactions if t['is_suspicious']):,}"
        )
        print(f"   • Whale Accounts: {len(self.whale_accounts)}")
        print(f"   • Regular Accounts: {len(self.regular_accounts)}")
        print(f"   • Thai Receivers: {len(self.thai_receivers)}")

        print(f"\n📈 REAL DATA FETCHED (For Business Questions 3,4):")
        crypto_prices = data["crypto_prices"]
        print(f"   • Price Points: {len(crypto_prices.get('prices', {}))}")
        print(f"   • THB Pairs: {len(crypto_prices.get('thb_pairs', {}))}")
        print(f"   • Trading Volumes: {len(crypto_prices.get('trading_volumes', {}))}")

        print(f"\n🎯 BUSINESS QUESTION COVERAGE:")
        print(f"   ✅ Cross-border Transfer Value (Q1)")
        print(f"   ✅ Top Accounts by Volume (Q2)")
        print(f"   ✅ Crypto Price History & Trends (Q3)")
        print(f"   ✅ Trading Transaction Counts (Q4)")
        print(f"   ✅ Top Senders Global→TH (Q5)")
        print(f"   ✅ Top Receivers Global→TH (Q6)")
        print(f"   ✅ Fraud Detection Patterns (Q7)")

        print(f"\n💾 DATA FILES CREATED:")
        print(f"   📄 transactions.json")
        print(f"   📄 trading_data.json")
        print(f"   📄 crypto_prices.json")
        print(f"   📄 business_summaries.json")

        print("=" * 60)


async def main():
    """Main execution function"""
    print("🚀 Starting CryptoBridge Advanced Data Generation...")

    generator = CryptoBridgeDataGenerator()

    # Generate complete dataset
    data = await generator.generate_complete_dataset(num_transactions=50000)

    # Print report
    generator.print_generation_report(data)

    print("\n🎉 Data generation complete!")
    print("Next steps:")
    print("1. Load data into ClickHouse: python init_clickhouse.py")
    print("2. Run producers to stream data: python kafka_transaction_producer.py")
    print("3. Set up Metabase for BI dashboards")


if __name__ == "__main__":
    asyncio.run(main())
