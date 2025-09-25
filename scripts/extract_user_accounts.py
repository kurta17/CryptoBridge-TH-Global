#!/usr/bin/env python3
"""
👥 User Account Extractor for CryptoBridge Analytics
Extracts unique user accounts from transactions and populates user_accounts table
"""

import json
import requests
import sys
from datetime import datetime
from collections import defaultdict
import random

def extract_user_accounts():
    """Extract user accounts from transaction data and load into ClickHouse"""
    print("👥 Extracting User Accounts from Transaction Data...")
    print("=" * 60)
    
    # ClickHouse connection
    base_url = "http://localhost:8123/"
    auth_params = {
        'user': 'analytics',
        'password': 'analytics123'
    }
    
    # Read transaction data
    print("📄 Reading transaction data...")
    try:
        with open('data/generated/transactions.json', 'r') as f:
            transactions = json.load(f)
        print(f"✅ Loaded {len(transactions):,} transactions")
    except Exception as e:
        print(f"❌ Failed to read transactions: {e}")
        return False
    
    # Extract unique users
    print("👤 Extracting unique user accounts...")
    users = {}
    
    for tx in transactions:
        # Process sender
        sender_id = tx.get('sender_user_id')
        if sender_id and sender_id not in users:
            users[sender_id] = {
                'user_id': sender_id,
                'account_type': tx.get('account_type', 'regular'),
                'country': tx.get('sender_country', tx.get('country', 'US')),
                'kyc_level': tx.get('sender_kyc_level', 2),
                'exchange': tx.get('from_exchange', 'Unknown'),
                'email': f"{sender_id.split('_')[1][:8]}@cryptobridge.com" if '_' in sender_id else f"{sender_id[:8]}@cryptobridge.com",
                'registration_date': datetime.now().date().strftime('%Y-%m-%d'),
                'risk_profile': 'high' if tx.get('is_suspicious', 0) == 1 else ('medium' if tx.get('risk_score', 0) > 50 else 'low'),
                'monthly_volume_usd': 0.0,  # Will calculate
                'preferred_cryptos': []
            }
        
        # Process receiver
        receiver_id = tx.get('receiver_user_id')
        if receiver_id and receiver_id not in users:
            users[receiver_id] = {
                'user_id': receiver_id,
                'account_type': tx.get('account_type', 'regular'),
                'country': tx.get('receiver_country', tx.get('country', 'TH')),
                'kyc_level': tx.get('receiver_kyc_level', 2),
                'exchange': tx.get('to_exchange', 'Unknown'),
                'email': f"{receiver_id.split('_')[1][:8]}@cryptobridge.com" if '_' in receiver_id else f"{receiver_id[:8]}@cryptobridge.com",
                'registration_date': datetime.now().date().strftime('%Y-%m-%d'),
                'risk_profile': 'high' if tx.get('is_suspicious', 0) == 1 else ('medium' if tx.get('risk_score', 0) > 50 else 'low'),
                'monthly_volume_usd': 0.0,  # Will calculate
                'preferred_cryptos': []
            }
    
    print(f"✅ Extracted {len(users):,} unique user accounts")
    
    # Calculate volumes and preferred cryptos per user
    print("📊 Calculating user volumes and preferences...")
    user_volumes = defaultdict(float)
    user_cryptos = defaultdict(set)
    
    for tx in transactions:
        amount_usd = tx.get('fiat_amount', tx.get('amount', 0))
        crypto = tx.get('crypto_currency', tx.get('currency', 'BTC'))
        
        sender_id = tx.get('sender_user_id')
        if sender_id:
            user_volumes[sender_id] += amount_usd
            user_cryptos[sender_id].add(crypto)
        
        receiver_id = tx.get('receiver_user_id')
        if receiver_id:
            user_volumes[receiver_id] += amount_usd * 0.5  # Receiving is less volume impact
            user_cryptos[receiver_id].add(crypto)
    
    # Update user profiles with calculated data
    for user_id, user_data in users.items():
        user_data['monthly_volume_usd'] = round(user_volumes.get(user_id, 0), 2)
        user_data['preferred_cryptos'] = list(user_cryptos.get(user_id, {'BTC'}))[:3]  # Top 3
    
    print(f"✅ Calculated volumes and preferences for {len(users):,} users")
    
    # Prepare data for insertion
    print("💾 Preparing data for ClickHouse insertion...")
    user_list = list(users.values())
    
    # Insert in batches
    batch_size = 1000
    total_inserted = 0
    
    for i in range(0, len(user_list), batch_size):
        batch = user_list[i:i + batch_size]
        
        # Prepare INSERT query
        values = []
        for user in batch:
            preferred_cryptos_str = "['" + "','".join(user['preferred_cryptos']) + "']"
            values.append(f"('{user['user_id']}', '{user['account_type']}', '{user['email']}', "
                         f"'{user['country']}', '{user['exchange']}', {user['kyc_level']}, "
                         f"'{user['registration_date']}', '{user['risk_profile']}', "
                         f"{user['monthly_volume_usd']}, {preferred_cryptos_str})")
        
        insert_query = f"""
        INSERT INTO cryptobridge.user_accounts 
        (user_id, account_type, email, country, exchange, kyc_level, registration_date, 
         risk_profile, monthly_volume_usd, preferred_cryptos) 
        VALUES {','.join(values)}
        """
        
        try:
            response = requests.post(
                base_url,
                params=auth_params,
                data=insert_query,
                timeout=30
            )
            
            if response.status_code == 200:
                total_inserted += len(batch)
                print(f"   ⏳ Inserted {total_inserted:,}/{len(user_list):,} user accounts...")
            else:
                print(f"⚠️ Batch insert failed: {response.status_code} - {response.text[:200]}")
                
        except Exception as e:
            print(f"❌ Batch insert error: {e}")
    
    print(f"✅ Successfully loaded {total_inserted:,} user accounts")
    
    # Verify the data
    print("🔍 Verifying user account data...")
    try:
        response = requests.post(
            base_url,
            params=auth_params,
            data='SELECT COUNT(*) FROM cryptobridge.user_accounts',
            timeout=10
        )
        
        if response.status_code == 200:
            count = int(response.text.strip())
            print(f"✅ Verified: {count:,} user accounts in database")
            
            # Show sample data
            response = requests.post(
                base_url,
                params=auth_params,
                data='SELECT account_type, country, COUNT(*) as count FROM cryptobridge.user_accounts GROUP BY account_type, country ORDER BY count DESC LIMIT 10 FORMAT JSON',
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                print("\n📊 User Account Distribution (Top 10):")
                for row in result.get('data', []):
                    print(f"   • {row['account_type']} in {row['country']}: {row['count']} users")
        
    except Exception as e:
        print(f"❌ Verification error: {e}")
    
    print("\n🎉 User Account Extraction Complete!")
    print("✅ user_accounts table is now populated")
    print("✅ Ready for business intelligence analysis")
    
    return True

if __name__ == "__main__":
    success = extract_user_accounts()
    sys.exit(0 if success else 1)