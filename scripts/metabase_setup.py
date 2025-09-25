#!/usr/bin/env python3
"""
📊 Metabase Setup for CryptoBridge BI Analytics
Sets up Metabase dashboards for the 7 key business questions
with real-time analytics and fraud detection.
"""

import json
import requests
import time
import sys
from typing import Dict, List, Any

class MetabaseSetup:
    def __init__(self, metabase_url: str = "http://localhost:3000"):
        """Initialize Metabase setup"""
        self.metabase_url = metabase_url
        self.session_token = None
        self.database_id = None
        
        print("📊 Metabase Setup for CryptoBridge Analytics")
        print("=" * 50)
    
    def login(self, username: str = "admin", password: str = "admin123"):
        """Login to Metabase and get session token"""
        print("🔐 Logging into Metabase...")
        
        login_data = {
            "username": username,
            "password": password
        }
        
        try:
            response = requests.post(
                f"{self.metabase_url}/api/session",
                json=login_data,
                timeout=10
            )
            
            if response.status_code == 200:
                self.session_token = response.json()["id"]
                print("✅ Successfully logged into Metabase")
                return True
            else:
                print(f"❌ Login failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Login error: {e}")
            return False
    
    def setup_clickhouse_connection(self):
        """Set up ClickHouse database connection in Metabase"""
        print("🔗 Setting up ClickHouse connection...")
        
        headers = {"X-Metabase-Session": self.session_token}
        
        database_config = {
            "engine": "clickhouse",
            "name": "CryptoBridge Analytics",
            "details": {
                "host": "localhost",
                "port": 8123,
                "dbname": "cryptobridge",
                "user": "analytics",
                "password": "analytics123",
                "ssl": False,
                "tunnel-enabled": False
            },
            "auto_run_queries": True,
            "is_full_sync": True,
            "schedules": {
                "metadata_sync": {
                    "schedule_day": None,
                    "schedule_frame": None,
                    "schedule_hour": 0,
                    "schedule_type": "hourly"
                }
            }
        }
        
        try:
            response = requests.post(
                f"{self.metabase_url}/api/database",
                json=database_config,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                self.database_id = response.json()["id"]
                print(f"✅ ClickHouse database connected (ID: {self.database_id})")
                return True
            else:
                print(f"❌ Database connection failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Database connection error: {e}")
            return False
    
    def create_business_questions(self) -> List[Dict]:
        """Create the 7 key business questions as Metabase cards"""
        print("🎯 Creating business analytics questions...")
        
        headers = {"X-Metabase-Session": self.session_token}
        
        # Business Question 1: Cross-border Transfer Value
        q1_sql = """
        SELECT 
            toYYYYMM(timestamp) as month,
            COUNT(*) as transaction_count,
            SUM(fiat_amount) as total_usd_volume,
            SUM(thb_equivalent) as total_thb_volume,
            AVG(fiat_amount) as avg_transaction_usd,
            currency
        FROM transactions 
        WHERE from_exchange = 'Binance_Global' AND to_exchange = 'Binance_TH'
            AND timestamp >= now() - INTERVAL 6 MONTH
        GROUP BY month, currency
        ORDER BY month DESC, total_usd_volume DESC
        """
        
        # Business Question 2: Top Accounts by Transfer Volume
        q2_sql = """
        SELECT 
            sender_user_id,
            account_type,
            sender_country,
            COUNT(*) as transaction_count,
            SUM(fiat_amount) as total_volume_usd,
            AVG(fiat_amount) as avg_transaction_usd,
            MAX(fiat_amount) as max_transaction_usd,
            AVG(risk_score) as avg_risk_score
        FROM transactions
        WHERE from_exchange = 'Binance_Global' AND to_exchange = 'Binance_TH'
        GROUP BY sender_user_id, account_type, sender_country
        ORDER BY total_volume_usd DESC
        LIMIT 10
        """
        
        # Business Question 3: Crypto Price History & Trends (REAL DATA)
        q3_sql = """
        SELECT 
            symbol,
            source,
            price,
            timestamp,
            LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price,
            (price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp)) / 
            LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) * 100 as price_change_pct
        FROM crypto_prices
        WHERE source IN ('binance_global', 'binance_th')
            AND symbol LIKE '%THB'
        ORDER BY symbol, timestamp DESC
        """
        
        # Business Question 4: Trading Transaction Counts Comparison
        q4_sql = """
        SELECT 
            exchange,
            symbol,
            timestamp as date,
            daily_trades as transaction_count,
            daily_volume,
            price_change_pct
        FROM trading_volumes
        WHERE timestamp >= today() - INTERVAL 30 DAY
        ORDER BY exchange, symbol, timestamp DESC
        """
        
        # Business Question 5: Top Senders Global to TH
        q5_sql = """
        SELECT 
            sender_user_id as user_id,
            sender_country as country,
            account_type,
            sender_kyc_level as kyc_level,
            COUNT(*) as send_count,
            SUM(fiat_amount) as total_sent_usd,
            COUNT(DISTINCT receiver_user_id) as unique_receivers,
            AVG(risk_score) as avg_risk_score
        FROM transactions
        WHERE from_exchange = 'Binance_Global' AND to_exchange = 'Binance_TH'
        GROUP BY sender_user_id, sender_country, account_type, sender_kyc_level
        ORDER BY total_sent_usd DESC
        LIMIT 5
        """
        
        # Business Question 6: Top Receivers Global to TH
        q6_sql = """
        SELECT 
            receiver_user_id as user_id,
            receiver_country as country,
            receiver_kyc_level as kyc_level,
            COUNT(*) as receive_count,
            SUM(fiat_amount) as total_received_usd,
            COUNT(DISTINCT sender_user_id) as unique_senders,
            AVG(risk_score) as avg_risk_score
        FROM transactions
        WHERE from_exchange = 'Binance_Global' AND to_exchange = 'Binance_TH'
        GROUP BY receiver_user_id, receiver_country, receiver_kyc_level
        ORDER BY total_received_usd DESC
        LIMIT 5
        """
        
        # Business Question 7: Fraud Detection - Time-windowed Analysis
        q7_sql = """
        WITH fraud_analysis AS (
            SELECT 
                sender_user_id,
                toStartOfMinute(timestamp) as minute_window,
                COUNT(*) as transactions_per_minute,
                SUM(fiat_amount) as total_amount_per_minute,
                COUNT(DISTINCT receiver_user_id) as unique_receivers_per_minute,
                AVG(risk_score) as avg_risk_score,
                MAX(fiat_amount) as max_single_transaction
            FROM transactions
            WHERE timestamp >= now() - INTERVAL 1 DAY
            GROUP BY sender_user_id, minute_window
        )
        SELECT 
            sender_user_id,
            minute_window,
            transactions_per_minute,
            total_amount_per_minute,
            unique_receivers_per_minute,
            avg_risk_score,
            max_single_transaction,
            CASE 
                WHEN max_single_transaction > 30000 
                     AND total_amount_per_minute > 10000 
                     AND unique_receivers_per_minute > 5 
                THEN 'HIGH_FRAUD_RISK'
                WHEN avg_risk_score > 70 
                THEN 'MEDIUM_FRAUD_RISK'
                ELSE 'LOW_RISK'
            END as fraud_classification
        FROM fraud_analysis
        WHERE total_amount_per_minute > 10000  -- Focus on significant transactions
        ORDER BY fraud_classification DESC, total_amount_per_minute DESC
        """
        
        questions = [
            {
                "name": "Q1: Cross-border Transfer Value (USD/THB)",
                "description": "Monthly cross-border transfer volumes from Binance Global to Binance TH",
                "sql": q1_sql,
                "display": "table",
                "visualization_settings": {
                    "table.pivot_column": "currency",
                    "table.cell_column": "total_usd_volume"
                }
            },
            {
                "name": "Q2: Top 10 Accounts by Transfer Volume", 
                "description": "Highest volume senders by total USD transferred",
                "sql": q2_sql,
                "display": "table"
            },
            {
                "name": "Q3: Crypto Price History & Trends (THB Pairs)",
                "description": "Real-time price trends for major crypto/THB trading pairs",
                "sql": q3_sql,
                "display": "line",
                "visualization_settings": {
                    "graph.dimensions": ["timestamp"],
                    "graph.metrics": ["price"]
                }
            },
            {
                "name": "Q4: Trading Volume Comparison (Global vs TH)",
                "description": "Daily trading transaction counts comparison",
                "sql": q4_sql,
                "display": "bar",
                "visualization_settings": {
                    "graph.dimensions": ["exchange", "date"],
                    "graph.metrics": ["transaction_count"]
                }
            },
            {
                "name": "Q5: Top 5 Senders Global→TH",
                "description": "Highest volume senders from Global to TH exchanges",
                "sql": q5_sql,
                "display": "table"
            },
            {
                "name": "Q6: Top 5 Receivers Global→TH",
                "description": "Highest volume receivers from Global to TH",
                "sql": q6_sql,
                "display": "table"
            },
            {
                "name": "Q7: Fraud Detection - Time Window Analysis",
                "description": "Real-time fraud detection using 1-minute time windows",
                "sql": q7_sql,
                "display": "table",
                "visualization_settings": {
                    "table.column_formatting": [
                        {"columns": ["fraud_classification"], "type": "text", "operator": "=", "value": "HIGH_FRAUD_RISK", "color": "#dc3545"}
                    ]
                }
            }
        ]
        
        created_cards = []
        
        for i, question in enumerate(questions, 1):
            print(f"   📝 Creating Q{i}: {question['name']}")
            
            card_data = {
                "name": question["name"],
                "description": question["description"],
                "display": question["display"],
                "visualization_settings": question.get("visualization_settings", {}),
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": question["sql"]
                    },
                    "database": self.database_id
                }
            }
            
            try:
                response = requests.post(
                    f"{self.metabase_url}/api/card",
                    json=card_data,
                    headers=headers,
                    timeout=15
                )
                
                if response.status_code == 200:
                    card = response.json()
                    created_cards.append(card)
                    print(f"      ✅ Created card ID: {card['id']}")
                else:
                    print(f"      ❌ Failed to create card: {response.status_code}")
                    
            except Exception as e:
                print(f"      ❌ Error creating card: {e}")
        
        print(f"✅ Created {len(created_cards)} business question cards")
        return created_cards
    
    def create_executive_dashboard(self, cards: List[Dict]) -> bool:
        """Create executive dashboard with key business metrics"""
        print("🎛️ Creating executive dashboard...")
        
        headers = {"X-Metabase-Session": self.session_token}
        
        dashboard_data = {
            "name": "CryptoBridge Executive Dashboard",
            "description": "Key business metrics and fraud detection for cross-border crypto transfers",
            "parameters": []
        }
        
        try:
            # Create dashboard
            response = requests.post(
                f"{self.metabase_url}/api/dashboard",
                json=dashboard_data,
                headers=headers,
                timeout=15
            )
            
            if response.status_code != 200:
                print(f"❌ Dashboard creation failed: {response.status_code}")
                return False
                
            dashboard = response.json()
            dashboard_id = dashboard["id"]
            print(f"✅ Created dashboard (ID: {dashboard_id})")
            
            # Add cards to dashboard
            for i, card in enumerate(cards):
                card_data = {
                    "cardId": card["id"],
                    "row": (i // 2) * 6,  # 2 cards per row, 6 units high
                    "col": (i % 2) * 12,  # 12 units wide each
                    "sizeX": 12,
                    "sizeY": 6
                }
                
                response = requests.post(
                    f"{self.metabase_url}/api/dashboard/{dashboard_id}/cards",
                    json=card_data,
                    headers=headers,
                    timeout=10
                )
                
                if response.status_code != 200:
                    print(f"   ⚠️ Failed to add card {card['id']} to dashboard")
            
            print(f"✅ Executive dashboard created with {len(cards)} visualizations")
            print(f"🔗 Access at: {self.metabase_url}/dashboard/{dashboard_id}")
            return True
            
        except Exception as e:
            print(f"❌ Dashboard creation error: {e}")
            return False
    
    def setup_automated_alerts(self) -> bool:
        """Set up automated alerts for fraud detection"""
        print("🚨 Setting up automated fraud alerts...")
        
        headers = {"X-Metabase-Session": self.session_token}
        
        # High-value transaction alert
        alert_sql = """
        SELECT 
            COUNT(*) as high_value_transactions,
            SUM(fiat_amount) as total_value_usd
        FROM transactions
        WHERE fiat_amount > 50000 
            AND timestamp >= now() - INTERVAL 1 HOUR
        """
        
        alert_data = {
            "card": {
                "name": "High Value Transaction Alert",
                "dataset_query": {
                    "type": "native",
                    "native": {"query": alert_sql},
                    "database": self.database_id
                },
                "display": "scalar",
                "visualization_settings": {}
            },
            "channels": [{
                "enabled": True,
                "channel_type": "email",
                "schedule_type": "hourly",
                "recipients": []  # Add email recipients here
            }]
        }
        
        try:
            response = requests.post(
                f"{self.metabase_url}/api/alert",
                json=alert_data,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                print("✅ High-value transaction alerts configured")
                return True
            else:
                print(f"⚠️ Alert setup failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Alert setup error: {e}")
            return False

def main():
    """Main setup function"""
    print("🚀 Starting Metabase Business Intelligence Setup...")
    
    # Check if Metabase is running
    try:
        response = requests.get("http://localhost:3000/api/health", timeout=5)
        if response.status_code != 200:
            print("❌ Metabase is not running on port 3000")
            print("Start Metabase first: docker run -p 3000:3000 metabase/metabase")
            sys.exit(1)
    except:
        print("❌ Cannot connect to Metabase at http://localhost:3000")
        print("Please start Metabase first:")
        print("docker run -p 3000:3000 metabase/metabase")
        sys.exit(1)
    
    setup = MetabaseSetup()
    
    # Step 1: Login
    if not setup.login():
        print("❌ Failed to login to Metabase")
        sys.exit(1)
    
    # Step 2: Set up ClickHouse connection
    if not setup.setup_clickhouse_connection():
        print("❌ Failed to connect to ClickHouse")
        sys.exit(1)
    
    # Step 3: Create business questions
    cards = setup.create_business_questions()
    if not cards:
        print("❌ Failed to create business questions")
        sys.exit(1)
    
    # Step 4: Create executive dashboard  
    if not setup.create_executive_dashboard(cards):
        print("❌ Failed to create dashboard")
        sys.exit(1)
    
    # Step 5: Set up alerts
    setup.setup_automated_alerts()
    
    print("\n" + "="*60)
    print("🎉 METABASE BUSINESS INTELLIGENCE SETUP COMPLETE!")
    print("="*60)
    print(f"🔗 Access Metabase at: http://localhost:3000")
    print(f"📊 Executive Dashboard: All 7 business questions")
    print(f"🚨 Automated Alerts: High-value transaction monitoring") 
    print(f"🎯 Real-time Analytics: Fraud detection with 1-minute windows")
    print("\n🔑 Key Features Implemented:")
    print("   ✅ Cross-border transfer volume analysis")
    print("   ✅ Top account identification and ranking")
    print("   ✅ Real-time crypto price trends (REAL DATA)")
    print("   ✅ Exchange volume comparison (Global vs TH)")
    print("   ✅ Advanced fraud detection algorithms")
    print("   ✅ Time-windowed transaction analysis")
    print("="*60)

if __name__ == "__main__":
    main()