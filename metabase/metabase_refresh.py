#!/usr/bin/env python3
"""
Metabase Database Refresh & Dashboard Update Script
Forces Metabase to sync with the updated ClickHouse data
"""

import requests
import json
import asyncio
import aiohttp
import time

METABASE_URL = "http://localhost:3000"
ADMIN_EMAIL = "admin@cryptobridge.local"
ADMIN_PASSWORD = "CryptoBridge2024!"

class MetabaseRefresher:
    def __init__(self):
        self.session_token = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def login(self):
        """Login to get session token"""
        print("🔑 Logging into Metabase...")
        
        login_data = {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
        
        async with self.session.post(
            f"{METABASE_URL}/api/session",
            json=login_data,
            headers={"Content-Type": "application/json"}
        ) as response:
            if response.status == 200:
                result = await response.json()
                self.session_token = result.get("id")
                print("✅ Logged in successfully")
                return True
            else:
                print(f"❌ Login failed: {response.status}")
                return False
    
    def get_auth_headers(self):
        return {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.session_token
        }
    
    async def get_databases(self):
        """Get list of databases"""
        async with self.session.get(
            f"{METABASE_URL}/api/database",
            headers=self.get_auth_headers()
        ) as response:
            if response.status == 200:
                result = await response.json()
                return result.get("data", [])
            return []
    
    async def sync_database(self, database_id):
        """Force database schema sync"""
        print(f"🔄 Syncing database {database_id}...")
        
        async with self.session.post(
            f"{METABASE_URL}/api/database/{database_id}/sync_schema",
            headers=self.get_auth_headers()
        ) as response:
            if response.status == 200:
                print("✅ Database sync triggered")
                return True
            else:
                print(f"❌ Sync failed: {response.status}")
                return False
    
    async def rescan_database(self, database_id):
        """Force field value rescan"""
        print(f"🔍 Rescanning field values for database {database_id}...")
        
        async with self.session.post(
            f"{METABASE_URL}/api/database/{database_id}/rescan_values",
            headers=self.get_auth_headers()
        ) as response:
            if response.status == 200:
                print("✅ Field values rescan triggered")
                return True
            else:
                print(f"❌ Rescan failed: {response.status}")
                return False
    
    async def get_dashboards(self):
        """Get list of dashboards"""
        async with self.session.get(
            f"{METABASE_URL}/api/dashboard",
            headers=self.get_auth_headers()
        ) as response:
            if response.status == 200:
                result = await response.json()
                return result.get("data", [])
            return []
    
    async def refresh_dashboard_questions(self, dashboard_id):
        """Refresh all questions in a dashboard"""
        print(f"📊 Refreshing dashboard {dashboard_id} questions...")
        
        # Get dashboard details
        async with self.session.get(
            f"{METABASE_URL}/api/dashboard/{dashboard_id}",
            headers=self.get_auth_headers()
        ) as response:
            if response.status != 200:
                print(f"❌ Failed to get dashboard: {response.status}")
                return False
            
            dashboard = await response.json()
            cards = dashboard.get("dashcards", [])
            
            print(f"🔄 Found {len(cards)} cards to refresh")
            
            for card in cards:
                card_id = card.get("card", {}).get("id")
                if card_id:
                    # This triggers a refresh of the question's cached results
                    async with self.session.post(
                        f"{METABASE_URL}/api/card/{card_id}/refresh",
                        headers=self.get_auth_headers()
                    ) as refresh_response:
                        if refresh_response.status == 200:
                            print(f"✅ Refreshed card {card_id}")
                        else:
                            print(f"⚠️ Card {card_id} refresh: {refresh_response.status}")
            
            return True

async def main():
    """Main refresh function"""
    print("🚀 Metabase Database & Dashboard Refresh")
    print("=" * 50)
    
    async with MetabaseRefresher() as refresher:
        # Step 1: Login
        if not await refresher.login():
            print("❌ Login failed. Exiting.")
            return
        
        # Step 2: Get databases and sync
        databases = await refresher.get_databases()
        clickhouse_db = None
        
        for db in databases:
            if "ClickHouse" in db.get("name", ""):
                clickhouse_db = db
                break
        
        if not clickhouse_db:
            print("❌ ClickHouse database not found")
            return
        
        db_id = clickhouse_db["id"]
        print(f"🗄️ Found ClickHouse database: {clickhouse_db['name']} (ID: {db_id})")
        
        # Step 3: Force sync database schema
        await refresher.sync_database(db_id)
        
        # Wait for sync to complete
        print("⏳ Waiting for schema sync...")
        await asyncio.sleep(10)
        
        # Step 4: Rescan field values
        await refresher.rescan_database(db_id)
        
        # Wait for rescan
        print("⏳ Waiting for field values rescan...")
        await asyncio.sleep(10)
        
        # Step 5: Get and refresh dashboards
        dashboards = await refresher.get_dashboards()
        crypto_dashboards = [d for d in dashboards if "CryptoBridge" in d.get("name", "")]
        
        print(f"📊 Found {len(crypto_dashboards)} CryptoBridge dashboards")
        
        for dashboard in crypto_dashboards:
            dashboard_id = dashboard["id"]
            dashboard_name = dashboard["name"]
            print(f"🔄 Refreshing dashboard: {dashboard_name} (ID: {dashboard_id})")
            
            await refresher.refresh_dashboard_questions(dashboard_id)
            
            print(f"🌐 Dashboard URL: {METABASE_URL}/dashboard/{dashboard_id}")
        
        print("\n" + "=" * 50)
        print("✅ Metabase refresh complete!")
        print("🔗 Your updated dashboards should now show the new data:")
        print(f"   • 25,000 transactions")
        print(f"   • 2,400 crypto prices")  
        print(f"   • 2,000 user accounts")
        print(f"   • 12,600 trading volumes")
        print("\n💡 If charts still show old data, try:")
        print(f"   1. Hard refresh the browser (Cmd/Ctrl + Shift + R)")
        print(f"   2. Clear browser cache")
        print(f"   3. Wait a few more minutes for sync to complete")

if __name__ == "__main__":
    asyncio.run(main())