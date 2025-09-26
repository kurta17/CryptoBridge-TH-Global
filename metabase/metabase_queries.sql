-- 📊 CryptoBridge Business Intelligence SQL Queries
-- Copy these queries into Metabase to create powerful dashboards

-- =================================================================
-- 🌍 BUSINESS QUESTION 1: Cross-Border Transfer Analysis
-- =================================================================
-- Top cross-border transfer corridors by volume
SELECT 
    sender_country,
    receiver_country,
    COUNT(*) as transaction_count,
    SUM(amount) as total_volume_usd,
    AVG(amount) as avg_transaction_size,
    SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_count,
    ROUND(AVG(risk_score), 1) as avg_risk_score
FROM transactions 
WHERE sender_country != receiver_country
GROUP BY sender_country, receiver_country
ORDER BY total_volume_usd DESC
LIMIT 20;

-- =================================================================
-- 👑 BUSINESS QUESTION 2: Top Account Analysis  
-- =================================================================
-- Top users by transaction volume with account insights
SELECT 
    ua.user_id,
    ua.account_type,
    ua.country,
    ua.exchange,
    ua.kyc_level,
    ua.monthly_volume_usd,
    COUNT(t.transaction_id) as transaction_count,
    SUM(t.amount) as total_volume,
    AVG(t.risk_score) as avg_risk_score,
    array_toString(ua.preferred_cryptos) as preferred_cryptos
FROM user_accounts ua
LEFT JOIN transactions t ON (ua.user_id = t.sender_user_id OR ua.user_id = t.receiver_user_id)
GROUP BY ua.user_id, ua.account_type, ua.country, ua.exchange, ua.kyc_level, ua.monthly_volume_usd, ua.preferred_cryptos
ORDER BY total_volume DESC
LIMIT 50;

-- =================================================================
-- 💰 BUSINESS QUESTION 3: Crypto Price Trends (Real Data)
-- =================================================================
-- Latest crypto prices and 24h changes
WITH latest_prices AS (
    SELECT 
        symbol,
        price_usd,
        volume_24h,
        change_24h,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
    FROM crypto_prices
)
SELECT 
    symbol,
    ROUND(price_usd, 4) as current_price_usd,
    ROUND(volume_24h, 0) as volume_24h_usd,
    ROUND(change_24h, 2) as change_24h_percent,
    timestamp as last_updated
FROM latest_prices 
WHERE rn = 1
ORDER BY volume_24h DESC
LIMIT 20;

-- =================================================================
-- 📈 BUSINESS QUESTION 4: Trading Volume Analysis
-- =================================================================
-- Trading volume comparison across exchanges and time
SELECT 
    DATE(timestamp) as trading_date,
    exchange,
    symbol,
    SUM(volume_usd) as daily_volume,
    AVG(volume_usd) as avg_volume,
    COUNT(*) as trading_sessions
FROM trading_volumes 
GROUP BY DATE(timestamp), exchange, symbol
ORDER BY trading_date DESC, daily_volume DESC;

-- =================================================================
-- 🚨 BUSINESS QUESTION 5: Fraud Detection Analysis
-- =================================================================
-- Fraud patterns and suspicious transaction analysis
SELECT 
    DATE(timestamp) as transaction_date,
    sender_country,
    receiver_country,
    account_type,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_transactions,
    ROUND(SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate_percent,
    SUM(amount) as total_volume,
    SUM(CASE WHEN is_suspicious = 1 THEN amount ELSE 0 END) as suspicious_volume,
    AVG(risk_score) as avg_risk_score,
    MAX(risk_score) as max_risk_score
FROM transactions 
GROUP BY DATE(timestamp), sender_country, receiver_country, account_type
HAVING COUNT(*) >= 5
ORDER BY fraud_rate_percent DESC, total_volume DESC
LIMIT 100;

-- =================================================================
-- ⏰ BUSINESS QUESTION 6: Time-Windowed Fraud Detection
-- =================================================================
-- Fraud alerts for high-risk time windows (last 24 hours pattern)
SELECT 
    sender_user_id,
    sender_country,
    COUNT(*) as transactions_24h,
    SUM(amount) as volume_24h,
    AVG(risk_score) as avg_risk_score,
    SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_count,
    MAX(timestamp) as last_transaction,
    array_uniq(array_agg(currency)) as currencies_used
FROM transactions 
WHERE timestamp >= NOW() - INTERVAL '24 HOUR'
GROUP BY sender_user_id, sender_country
HAVING COUNT(*) >= 3 AND AVG(risk_score) >= 50
ORDER BY avg_risk_score DESC, volume_24h DESC
LIMIT 50;

-- =================================================================
-- 🎯 BUSINESS QUESTION 7: Executive KPI Dashboard
-- =================================================================
-- High-level executive summary metrics
SELECT 
    'Total Transactions' as metric,
    COUNT(*) as value,
    '$' || ROUND(SUM(amount), 0) as volume
FROM transactions
UNION ALL
SELECT 
    'Cross-Border Transfers' as metric,
    COUNT(*) as value,
    '$' || ROUND(SUM(amount), 0) as volume
FROM transactions 
WHERE sender_country != receiver_country
UNION ALL
SELECT 
    'Suspicious Transactions' as metric,
    SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as value,
    '$' || ROUND(SUM(CASE WHEN is_suspicious = 1 THEN amount ELSE 0 END), 0) as volume
FROM transactions
UNION ALL
SELECT 
    'Active Users' as metric,
    COUNT(*) as value,
    'N/A' as volume
FROM user_accounts
UNION ALL
SELECT 
    'Countries Served' as metric,
    COUNT(DISTINCT country) as value,
    'N/A' as volume
FROM user_accounts;

-- =================================================================
-- 💎 BONUS: Geographic Transaction Flow Analysis
-- =================================================================
-- Geographic flow analysis for network visualization
SELECT 
    sender_country as source,
    receiver_country as target,
    COUNT(*) as transaction_count,
    SUM(amount) as flow_volume,
    AVG(amount) as avg_amount,
    ROUND(AVG(risk_score), 1) as avg_risk
FROM transactions 
WHERE sender_country != receiver_country
GROUP BY sender_country, receiver_country
HAVING COUNT(*) >= 10
ORDER BY flow_volume DESC
LIMIT 100;

-- =================================================================
-- 🔥 BONUS: Real-Time Crypto Performance Dashboard
-- =================================================================
-- Crypto performance with volume and price correlation
SELECT 
    cp.symbol,
    ROUND(cp.price_usd, 4) as price,
    ROUND(cp.change_24h, 2) as change_24h_percent,
    cp.volume_24h as market_volume_24h,
    COUNT(t.transaction_id) as platform_transactions,
    SUM(t.amount) as platform_volume,
    ROUND(AVG(t.amount), 2) as avg_transaction_size
FROM crypto_prices cp
LEFT JOIN transactions t ON cp.symbol = t.crypto_currency
WHERE cp.timestamp >= NOW() - INTERVAL '1 DAY'
GROUP BY cp.symbol, cp.price_usd, cp.change_24h, cp.volume_24h
ORDER BY platform_volume DESC;