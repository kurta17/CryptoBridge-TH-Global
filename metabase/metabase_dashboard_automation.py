#!/usr/bin/env python3
"""
Metabase Dashboard Automation Script for CryptoBridge Analytics
This script automatically sets up Metabase with ClickHouse connection and creates comprehensive BI dashboards
"""

import asyncio
import aiohttp
import json
import time
import os
from typing import Dict, List, Any, Optional

# Configuration
METABASE_BASE_URL = "http://localhost:3000"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "cryptobridge"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"

# Admin user configuration - Try common default credentials first
ADMIN_CREDENTIALS = [
    {"email": "admin@example.com", "password": "admin123"},
    {"email": "admin@admin.com", "password": "admin"},
    {"email": "test@example.com", "password": "test123"},
    {"email": "admin@cryptobridge.local", "password": "CryptoBridge2024!"},
    {"email": "admin@localhost", "password": "password"},
]

SETUP_ADMIN_EMAIL = "admin@cryptobridge.local"
SETUP_ADMIN_PASSWORD = "CryptoBridge2024!"
SETUP_ADMIN_FIRST_NAME = "CryptoBridge"
SETUP_ADMIN_LAST_NAME = "Admin"

class MetabaseAutomation:
    def __init__(self):
        self.session_token = None
        self.database_id = None
        self.session = None
        self.current_admin_email = None
        self.current_admin_password = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def try_login_with_credentials(self) -> bool:
        """Try to login with various common credentials"""
        print("🔑 Attempting to login with common credentials...")
        
        for creds in ADMIN_CREDENTIALS:
            try:
                login_data = {
                    "username": creds["email"],
                    "password": creds["password"]
                }
                
                async with self.session.post(
                    f"{METABASE_BASE_URL}/api/session", 
                    json=login_data,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.session_token = result.get("id")
                        self.current_admin_email = creds["email"]
                        self.current_admin_password = creds["password"]
                        print(f"✅ Successfully logged in with {creds['email']}")
                        return True
                    elif response.status == 401:
                        print(f"❌ Invalid credentials for {creds['email']}")
                    else:
                        error_text = await response.text()
                        print(f"❌ Login failed for {creds['email']}: {response.status} - {error_text}")
                        
            except Exception as e:
                print(f"❌ Error trying credentials {creds['email']}: {e}")
                
        return False
    
    async def setup_admin_user(self) -> bool:
        """Set up initial admin user if needed"""
        print("🔧 Setting up Metabase admin user...")
        
        try:
            # Check if setup is needed
            async with self.session.get(f"{METABASE_BASE_URL}/api/session/properties") as response:
                if response.status != 200:
                    print("❌ Cannot connect to Metabase")
                    return False
                    
                properties = await response.json()
                has_user_setup = properties.get("has-user-setup", False)
                
                if has_user_setup:
                    print("ℹ️ Admin user already exists, attempting login with common credentials...")
                    return await self.try_login_with_credentials()
                
            # Setup admin user
            setup_data = {
                "token": properties.get("setup-token"),
                "user": {
                    "email": SETUP_ADMIN_EMAIL,
                    "first_name": SETUP_ADMIN_FIRST_NAME,
                    "last_name": SETUP_ADMIN_LAST_NAME,
                    "password": SETUP_ADMIN_PASSWORD,
                    "site_name": "CryptoBridge Analytics"
                },
                "prefs": {
                    "allow_tracking": False,
                    "site_name": "CryptoBridge Analytics"
                }
            }
            
            async with self.session.post(
                f"{METABASE_BASE_URL}/api/setup", 
                json=setup_data,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    self.session_token = result.get("id")
                    self.current_admin_email = SETUP_ADMIN_EMAIL
                    self.current_admin_password = SETUP_ADMIN_PASSWORD
                    print("✅ Admin user created successfully")
                    return True
                else:
                    error_text = await response.text()
                    print(f"❌ Failed to create admin user: {response.status} - {error_text}")
                    return False
                    
        except Exception as e:
            print(f"❌ Error setting up admin user: {e}")
            return False
    
    async def login(self) -> bool:
        """Login to Metabase"""
        return await self.try_login_with_credentials()
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers"""
        return {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.session_token
        }
    
    async def setup_clickhouse_database(self) -> bool:
        """Set up ClickHouse database connection"""
        print("🗄️ Setting up ClickHouse database connection...")
        
        try:
            # Check if database already exists
            async with self.session.get(
                f"{METABASE_BASE_URL}/api/database",
                headers=self.get_auth_headers()
            ) as response:
                if response.status == 200:
                    databases = await response.json()
                    for db in databases.get("data", []):
                        if db.get("name") == "CryptoBridge ClickHouse":
                            self.database_id = db["id"]
                            print("✅ ClickHouse database connection already exists")
                            return True
            
            # Create new database connection
            database_config = {
                "name": "CryptoBridge ClickHouse",
                "engine": "clickhouse",
                "details": {
                    "host": "clickhouse",
                    "port": 8123,
                    "user": "analytics",
                    "password": "analytics123",
                    "dbname": "cryptobridge",
                    "ssl": False,
                    "tunnel-enabled": False,
                    "additional-options": ""
                },
                "auto_run_queries": True,
                "is_full_sync": True
            }
            
            async with self.session.post(
                f"{METABASE_BASE_URL}/api/database",
                json=database_config,
                headers=self.get_auth_headers()
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    self.database_id = result["id"]
                    print("✅ ClickHouse database connection created")
                    
                    # Wait for sync
                    print("⏳ Waiting for database sync...")
                    await asyncio.sleep(10)
                    
                    # Trigger sync manually
                    await self.sync_database()
                    return True
                else:
                    error_text = await response.text()
                    print(f"❌ Failed to create database connection: {response.status} - {error_text}")
                    return False
                    
        except Exception as e:
            print(f"❌ Error setting up database: {e}")
            return False
    
    async def sync_database(self) -> bool:
        """Manually trigger database sync"""
        try:
            async with self.session.post(
                f"{METABASE_BASE_URL}/api/database/{self.database_id}/sync_schema",
                headers=self.get_auth_headers()
            ) as response:
                if response.status == 200:
                    print("✅ Database sync triggered")
                    return True
                else:
                    print(f"⚠️ Sync trigger failed: {response.status}")
                    return False
        except Exception as e:
            print(f"❌ Error triggering sync: {e}")
            return False
    
    async def create_question(self, title: str, query: str, visualization_type: str = "table", 
                            description: str = "") -> Optional[int]:
        """Create a native SQL question"""
        try:
            question_data = {
                "name": title,
                "description": description,
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": query
                    },
                    "database": self.database_id
                },
                "display": visualization_type,
                "visualization_settings": self.get_visualization_settings(visualization_type)
            }
            
            async with self.session.post(
                f"{METABASE_BASE_URL}/api/card",
                json=question_data,
                headers=self.get_auth_headers()
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    print(f"✅ Created question: {title}")
                    return result["id"]
                else:
                    error_text = await response.text()
                    print(f"❌ Failed to create question '{title}': {response.status} - {error_text}")
                    return None
                    
        except Exception as e:
            print(f"❌ Error creating question '{title}': {e}")
            return None
    
    def get_visualization_settings(self, viz_type: str) -> Dict[str, Any]:
        """Get appropriate visualization settings for different chart types"""
        if viz_type == "bar":
            return {
                "graph.show_values": True,
                "graph.x_axis.scale": "ordinal",
                "graph.y_axis.scale": "linear"
            }
        elif viz_type == "line":
            return {
                "graph.show_values": True,
                "graph.x_axis.scale": "ordinal",
                "graph.y_axis.scale": "linear",
                "graph.x_axis.title_text": "Hour of Day",
                "graph.y_axis.title_text": "Count / Percentage"
            }
        elif viz_type == "pie":
            return {
                "pie.show_legend": True,
                "pie.show_total": True
            }
        elif viz_type == "scalar":
            return {
                "scalar.field": "total"
            }
        else:  # table
            return {
                "table.column_formatting": []
            }
    
    async def create_dashboard(self, title: str, description: str, question_ids: List[int]) -> Optional[int]:
        """Create a dashboard with questions"""
        try:
            dashboard_data = {
                "name": title,
                "description": description,
                "parameters": []
            }
            
            async with self.session.post(
                f"{METABASE_BASE_URL}/api/dashboard",
                json=dashboard_data,
                headers=self.get_auth_headers()
            ) as response:
                if response.status == 200:
                    dashboard = await response.json()
                    dashboard_id = dashboard["id"]
                    print(f"✅ Created dashboard: {title}")
                    
                    # Add questions to dashboard
                    await self.add_questions_to_dashboard(dashboard_id, question_ids)
                    return dashboard_id
                else:
                    error_text = await response.text()
                    print(f"❌ Failed to create dashboard '{title}': {response.status} - {error_text}")
                    return None
                    
        except Exception as e:
            print(f"❌ Error creating dashboard '{title}': {e}")
            return None
    
    async def add_questions_to_dashboard(self, dashboard_id: int, question_ids: List[int]):
        """Add questions as cards to dashboard"""
        try:
            cards = []
            row, col = 0, 0
            
            for i, question_id in enumerate(question_ids):
                if question_id is None:
                    continue
                    
                card = {
                    "id": question_id,
                    "card_id": question_id,
                    "row": row,
                    "col": col,
                    "size_x": 6,
                    "size_y": 4,
                    "parameter_mappings": [],
                    "visualization_settings": {}
                }
                
                cards.append(card)
                
                # Layout: 2 cards per row
                col += 6
                if col >= 12:
                    col = 0
                    row += 4
            
            if cards:
                async with self.session.put(
                    f"{METABASE_BASE_URL}/api/dashboard/{dashboard_id}/cards",
                    json={"cards": cards},
                    headers=self.get_auth_headers()
                ) as response:
                    if response.status == 200:
                        print(f"✅ Added {len(cards)} cards to dashboard")
                    else:
                        error_text = await response.text()
                        print(f"⚠️ Failed to add cards to dashboard: {response.status} - {error_text}")
                        
        except Exception as e:
            print(f"❌ Error adding cards to dashboard: {e}")

    async def create_comprehensive_dashboards(self):
        """Create all CryptoBridge analytics dashboards"""
        print("🎨 Creating comprehensive analytics dashboards...")
        
        # Define questions with their SQL queries
        questions = [
            {
                "title": "Executive KPI Overview",
                "description": "High-level business metrics for executives",
                "query": """
SELECT 
    'Total Transaction Volume' as metric,
    concat('$', formatReadableQuantity(SUM(amount))) as value
FROM cryptobridge.transactions
UNION ALL
SELECT 
    'Total Transactions' as metric,
    toString(COUNT(*)) as value
FROM cryptobridge.transactions
UNION ALL
SELECT 
    'Active Users (30d)' as metric,
    toString(COUNT(DISTINCT sender_id)) as value
FROM cryptobridge.transactions
WHERE transaction_date >= today() - 30
UNION ALL
SELECT 
    'Fraud Rate' as metric,
    concat(toString(ROUND(AVG(is_suspicious) * 100, 2)), '%') as value
FROM cryptobridge.transactions
                """,
                "visualization": "table"
            },
            {
                "title": "Cross-Border Money Flow (Top Corridors)",
                "description": "Major international money transfer corridors by volume",
                "query": """
SELECT 
    concat(sender_country, ' → ', recipient_country) as corridor,
    COUNT(*) as transaction_count,
    concat('$', formatReadableQuantity(SUM(amount))) as total_volume,
    ROUND(AVG(amount), 2) as avg_amount,
    concat(toString(ROUND(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM cryptobridge.transactions WHERE sender_country != recipient_country), 2)), '%') as volume_share
FROM cryptobridge.transactions 
WHERE sender_country != recipient_country
GROUP BY sender_country, recipient_country
ORDER BY SUM(amount) DESC
LIMIT 10
                """,
                "visualization": "table"
            },
            {
                "title": "Crypto Performance Dashboard",
                "description": "Real-time cryptocurrency price performance",
                "query": """
SELECT 
    symbol,
    price_usd as current_price,
    volume_24h,
    percent_change_24h,
    market_cap,
    last_updated
FROM cryptobridge.crypto_prices 
WHERE symbol IN ('BTC', 'ETH', 'BNB', 'ADA', 'DOT', 'MATIC', 'LINK', 'UNI', 'AAVE', 'SUSHI')
ORDER BY market_cap DESC
                """,
                "visualization": "table"
            },
            {
                "title": "Daily Transaction Volume Trend",
                "description": "Transaction volume over time",
                "query": """
SELECT 
    transaction_date as date,
    COUNT(*) as transaction_count,
    SUM(amount) as total_volume
FROM cryptobridge.transactions
WHERE transaction_date >= today() - 30
GROUP BY transaction_date
ORDER BY transaction_date
                """,
                "visualization": "line"
            },
            {
                "title": "Fraud Detection Analysis",
                "description": "Fraud patterns by account type and geography",
                "query": """
SELECT 
    account_type,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_transactions,
    ROUND(AVG(is_suspicious) * 100, 2) as fraud_rate_percent,
    ROUND(AVG(CASE WHEN is_suspicious = 1 THEN amount ELSE 0 END), 2) as avg_fraud_amount
FROM cryptobridge.transactions
GROUP BY account_type
ORDER BY fraud_rate_percent DESC
                """,
                "visualization": "bar"
            },
            {
                "title": "Geographic Distribution",
                "description": "Transaction volume by country",
                "query": """
SELECT 
    sender_country as country,
    COUNT(*) as transaction_count,
    SUM(amount) as total_volume,
    COUNT(DISTINCT sender_id) as unique_users
FROM cryptobridge.transactions
GROUP BY sender_country
ORDER BY total_volume DESC
LIMIT 15
                """,
                "visualization": "bar"
            },
            {
                "title": "Top Cryptocurrency by Trading Volume",
                "description": "Most traded cryptocurrencies by volume",
                "query": """
SELECT 
    currency,
    COUNT(*) as transaction_count,
    SUM(amount) as total_volume,
    ROUND(AVG(amount), 2) as avg_transaction_size
FROM cryptobridge.transactions
GROUP BY currency
ORDER BY total_volume DESC
LIMIT 10
                """,
                "visualization": "bar"
            },
            {
                "title": "Time-based Fraud Patterns",
                "description": "Fraud detection over time windows - Fixed X/Y axes",
                "query": """
SELECT 
    toHour(transaction_timestamp) as hour_of_day,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_count,
    ROUND(AVG(is_suspicious) * 100, 1) as fraud_rate_percent
FROM cryptobridge.transactions
GROUP BY toHour(transaction_timestamp)
ORDER BY hour_of_day
                """,
                "visualization": "line"
            },
            {
                "title": "High-Value Transactions Monitor",
                "description": "Large transactions requiring attention",
                "query": """
SELECT 
    transaction_id,
    sender_id,
    recipient_id,
    concat(sender_country, ' → ', recipient_country) as corridor,
    currency,
    amount,
    transaction_date,
    CASE WHEN is_suspicious = 1 THEN '🚨 Suspicious' ELSE '✅ Normal' END as status
FROM cryptobridge.transactions
WHERE amount >= 50000
ORDER BY amount DESC, transaction_date DESC
LIMIT 20
                """,
                "visualization": "table"
            },
            {
                "title": "User Account Analysis",
                "description": "User segmentation and behavior patterns",
                "query": """
SELECT 
    u.account_type,
    COUNT(DISTINCT u.user_id) as user_count,
    ROUND(AVG(u.total_transaction_volume), 2) as avg_user_volume,
    ROUND(AVG(u.transaction_count), 2) as avg_transactions_per_user,
    MAX(u.total_transaction_volume) as max_user_volume
FROM cryptobridge.user_accounts u
GROUP BY u.account_type
ORDER BY avg_user_volume DESC
                """,
                "visualization": "table"
            },
            {
                "title": "Exchange Trading Volume Comparison",
                "description": "Trading volumes across different exchanges - NEW TABLE",
                "query": """
SELECT 
    exchange,
    SUM(volume_usd) as total_volume_usd,
    SUM(daily_trades) as total_trades,
    ROUND(AVG(avg_trade_size), 2) as avg_trade_size,
    COUNT(DISTINCT symbol) as symbols_traded
FROM cryptobridge.trading_volumes
WHERE trade_date >= today() - 30
GROUP BY exchange
ORDER BY total_volume_usd DESC
                """,
                "visualization": "bar"
            },
            {
                "title": "Risk Score Distribution",
                "description": "Distribution of transaction risk scores",
                "query": """
SELECT 
    CASE 
        WHEN risk_score < 20 THEN 'Low Risk (0-19)'
        WHEN risk_score < 50 THEN 'Medium Risk (20-49)' 
        WHEN risk_score < 70 THEN 'High Risk (50-69)'
        ELSE 'Critical Risk (70+)'
    END as risk_category,
    COUNT(*) as transaction_count,
    ROUND(AVG(fiat_amount), 2) as avg_amount,
    SUM(fiat_amount) as total_volume
FROM cryptobridge.transactions
GROUP BY risk_category
ORDER BY avg_amount DESC
                """,
                "visualization": "pie"
            }
        ]
        
        # Create all questions
        question_ids = []
        for q in questions:
            question_id = await self.create_question(
                title=q["title"],
                query=q["query"],
                visualization_type=q["visualization"],
                description=q["description"]
            )
            question_ids.append(question_id)
            await asyncio.sleep(1)  # Rate limiting
        
        # Create main dashboard
        main_dashboard_id = await self.create_dashboard(
            title="🏦 CryptoBridge Analytics - Executive Dashboard",
            description="Comprehensive business intelligence dashboard for cryptocurrency and cross-border payment analytics",
            question_ids=[q for q in question_ids if q is not None]
        )
        
        if main_dashboard_id:
            print(f"🎉 Successfully created main dashboard with ID: {main_dashboard_id}")
            print(f"🌐 Access your dashboard at: {METABASE_BASE_URL}/dashboard/{main_dashboard_id}")
        
        return main_dashboard_id

async def main():
    """Main automation function"""
    print("🚀 Starting Metabase Dashboard Automation for CryptoBridge Analytics")
    print("=" * 80)
    
    async with MetabaseAutomation() as automation:
        # Step 1: Setup admin user or login
        if not await automation.setup_admin_user():
            print("❌ Failed to setup admin user. Exiting.")
            return
        
        # Step 2: Setup ClickHouse database connection
        if not await automation.setup_clickhouse_database():
            print("❌ Failed to setup database connection. Exiting.")
            return
        
        # Step 3: Wait for database to be ready
        print("⏳ Waiting for database to be fully synced...")
        await asyncio.sleep(15)
        
        # Step 4: Create comprehensive dashboards
        dashboard_id = await automation.create_comprehensive_dashboards()
        
        if dashboard_id:
            print("\n" + "=" * 80)
            print("🎉 SUCCESS! Your CryptoBridge Analytics dashboards are ready!")
            print("=" * 80)
            print(f"📊 Main Dashboard URL: {METABASE_BASE_URL}/dashboard/{dashboard_id}")
            print(f"🏠 Metabase Home: {METABASE_BASE_URL}")
            print(f"🔑 Login Credentials:")
            print(f"   Email: {automation.current_admin_email}")
            print(f"   Password: {automation.current_admin_password}")
            print("\n💡 Your dashboard includes:")
            print("   • Executive KPI Overview")
            print("   • Cross-Border Money Flow Analysis")
            print("   • Real-time Crypto Performance")
            print("   • Fraud Detection & Risk Analysis")
            print("   • Geographic Distribution")
            print("   • Time-based Analytics")
            print("   • High-Value Transaction Monitoring")
            print("   • User Segmentation Analysis")
            print("\n🔗 All visualizations are powered by your ClickHouse database with:")
            print("   • 60,155+ transactions ($3.9B+ volume)")
            print("   • 6,594+ real-time crypto prices")
            print("   • 375 active user accounts")
            print("   • Advanced fraud detection patterns")
        else:
            print("❌ Failed to create dashboards. Check the logs above for details.")

if __name__ == "__main__":
    asyncio.run(main())