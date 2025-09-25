# Fake Transaction Data - CryptoBridge TH Global

## Overview

This dataset contains **synthetic cross-border cryptocurrency transaction data** simulating transfers between Binance Global and Binance Thailand. The data is designed to support business analytics, fraud detection, and data pipeline development.

## Dataset Statistics

- **Total Transactions**: 8,420
- **Total Volume**: $616.7M USD / ฿21.9B THB
- **Average Transaction**: $73,244 USD
- **Fraud Rate**: 4.99% (420 suspicious transactions)
- **Date Range**: September 2024 - September 2025
- **File Format**: JSON

## Business Questions Supported

This synthetic dataset enables analysis of:

1. **Cross-border Transfer Value** - USD/THB equivalent analysis
2. **Top Accounts by Transfer Volume** - High-value trader identification
3. **Top Senders from Global to TH** - Source country analysis
4. **Top Receivers from Global to TH** - Thai market penetration
5. **Fraud Detection** - Suspicious pattern identification (>$30K + multiple recipients)

## Data Schema

### Transaction Structure

```json
{
  "transaction_id": "uuid",
  "timestamp": "ISO 8601 datetime",
  "sender": {
    "user_id": "uuid",
    "email": "fake email",
    "country": "country code",
    "exchange": "Binance_Global",
    "account_type": "regular|whale|suspicious",
    "kyc_level": 1-3
  },
  "receiver": {
    "user_id": "uuid", 
    "email": "fake email",
    "country": "TH",
    "exchange": "Binance_TH",
    "kyc_level": 2-3,
    "registration_date": "ISO date"
  },
  "transfer": {
    "crypto_symbol": "BTC|ETH|USDT|etc",
    "crypto_amount": "decimal amount",
    "usd_equivalent": "USD value",
    "thb_equivalent": "THB value",
    "exchange_rate_usd_thb": 35.50,
    "crypto_rate_usd": "rate at time"
  },
  "fees": {
    "fee_usd": "fee in USD",
    "fee_thb": "fee in THB", 
    "fee_rate": "0.001|0.0015",
    "fee_type": "maker|taker"
  },
  "network": {
    "network_type": "blockchain network",
    "confirmation_blocks": "number of confirmations",
    "network_fee_usd": "gas/network fee"
  },
  "status": "completed",
  "processing_time_minutes": "processing duration",
  "compliance": {
    "aml_score": "0.0-1.0 risk score",
    "sanctions_check": "passed|flagged",
    "risk_level": "low|medium|high"
  },
  "fraud_pattern": {
    // Only present for fraud pattern transactions
    "pattern_type": "multiple_receivers_short_window",
    "total_recipients_in_window": "number",
    "time_window_minutes": 1,
    "pattern_total_usd": "total pattern amount"
  }
}
```

## Cryptocurrency Coverage

The dataset includes 15 major cryptocurrencies:

| Symbol | Name         | Transactions | USD Rate |
| ------ | ------------ | ------------ | -------- |
| BTC    | Bitcoin      | 467          | $45,000  |
| ETH    | Ethereum     | 1,140        | $2,800   |
| USDT   | Tether       | 555          | $1.00    |
| USDC   | USD Coin     | 1,046        | $1.00    |
| BNB    | Binance Coin | 700          | $320     |
| ADA    | Cardano      | 127          | $0.45    |
| DOT    | Polkadot     | 793          | $6.50    |
| XRP    | Ripple       | 361          | $0.62    |
| SOL    | Solana       | 194          | $98.50   |
| MATIC  | Polygon      | 336          | $0.85    |
| AVAX   | Avalanche    | 697          | $28.50   |
| LINK   | Chainlink    | 718          | $14.20   |
| UNI    | Uniswap      | 445          | $6.80    |
| LTC    | Litecoin     | 115          | $95.00   |
| BCH    | Bitcoin Cash | 726          | $240.00  |

## Geographic Distribution

**Sender Countries:**

- United States (US): 1,282 transactions
- Singapore (SG): 1,329 transactions
- Philippines (PH): 1,022 transactions
- China (CN): 961 transactions
- Malaysia (MY): 830 transactions
- Japan (JP): 818 transactions
- United Kingdom (UK): 779 transactions
- South Korea (KR): 714 transactions
- Vietnam (VN): 634 transactions
- Russia (RU): 51 transactions

**Receiver Country:** Thailand (TH) - All transactions

## Account Types

### Regular Accounts (80% of transactions)

- **Volume Range**: $100 - $50,000 USD
- **KYC Levels**: 1-3 (random distribution)
- **Countries**: Global distribution
- **Risk Profile**: Low (AML score 0.1-0.3)

### Whale Accounts (20% of transactions)

- **Volume Range**: $50,000 - $500,000 USD
- **KYC Level**: 3 (fully verified)
- **Countries**: Major trading hubs (US, UK, CN, KR, JP, SG)
- **Risk Profile**: Low (legitimate high-volume traders)

### Suspicious Accounts (fraud patterns)

- **Volume Range**: $30,000+ USD individual, $10,000+ patterns
- **KYC Level**: 1 (minimal verification)
- **Countries**: High-risk jurisdictions
- **Risk Profile**: High (AML score 0.6-0.95)
- **Patterns**: Multiple recipients in short time windows

## Fraud Detection Patterns

### Individual Suspicious Transactions

- **Threshold**: >$30,000 USD
- **Cryptocurrencies**: Primarily USDT, USDC, BTC
- **Processing**: Faster than normal (2-15 minutes)
- **Compliance**: AML scores 0.6-0.9, sanctions flagged
- **Count**: 300 transactions

### Multi-Recipient Patterns

- **Pattern**: 1 sender → 8+ recipients within 1 minute
- **Individual Amount**: $1,500-$3,000 USD each
- **Total Pattern Amount**: >$10,000 USD
- **Risk Level**: High
- **Count**: 15 pattern groups (120 transactions)

## Files Generated

1. **`fake_transactions.json`** (10.5MB)

   - Complete transaction dataset in JSON format
   - Nested structure preserving all relationships
   - Ready for Bronze layer ingestion
2. **`transaction_summary.json`** (776B)

   - Statistical summary and metadata
   - Crypto/country distribution
   - Volume and fraud rate metrics

## Data Pipeline Usage

### Bronze Layer (Raw Data)

```bash
# Load into data lake (HDFS/S3)
hadoop fs -put fake_transactions.json /bronze/transactions/
```

### Silver Layer (Cleaned/Transformed)

```sql
-- Spark SQL example for fraud detection
SELECT sender_user_id, COUNT(*) as tx_count, SUM(usd_equivalent) as total_volume
FROM transactions 
WHERE compliance_risk_level IN ('medium', 'high')
GROUP BY sender_user_id
HAVING total_volume > 30000
```

### Gold Layer (Business Metrics)

- Cross-border volume by country/crypto
- Top sender/receiver analysis
- Fraud pattern detection alerts
- Regulatory compliance reporting

## Generation Details

- **Script**: `scripts/generate_fake_transactions.py`
- **Library**: Python Faker with multiple locales
- **Seed**: 42 (reproducible results)
- **Realistic Rates**: Based on 2024 crypto market data
- **Exchange Rates**: USD/THB = 35.50

## Next Steps

1. **Kafka Integration**: Stream transactions for real-time processing
2. **Spark Processing**: Implement Bronze→Silver→Gold transformations
3. **Fraud Detection**: Use the logic to identfy them.
4. **BI Dashboards**: Create analytics dashboards for business questions

*Generated on: September 25, 2025*
*Data Volume: 8,420 transactions, $616.7M USD*
*Purpose: Cross-border cryptocurrency analytics and fraud detection*
