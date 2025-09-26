-- 📊 CryptoBridge Business Intelligence - Ready-to-Use Metabase Visualizations
-- Copy these queries into Metabase "Native Query" to create stunning dashboards

-- =================================================================
-- 🌍 VISUALIZATION 1: Cross-Border Money Flow (Horizontal Bar Chart)
-- =================================================================
-- TOP CROSS-BORDER CORRIDORS - Perfect for Horizontal Bar Chart
SELECT 
    CONCAT(sender_country, ' → ', receiver_country) as corridor,
    COUNT(*) as transactions,
    toUInt64(SUM(amount)) as volume_usd,
    ROUND(AVG(amount), 0) as avg_transaction
FROM cryptobridge.transactions 
WHERE sender_country != receiver_country
GROUP BY sender_country, receiver_country
ORDER BY volume_usd DESC
LIMIT 15;

-- =================================================================
-- 🚨 VISUALIZATION 2: Fraud Detection Dashboard (Multi-series Chart)
-- =================================================================
-- ACCOUNT TYPE RISK ANALYSIS - Perfect for Grouped Bar Chart
SELECT 
    account_type,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_transactions,
    ROUND(SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as fraud_rate_percent,
    ROUND(AVG(risk_score), 1) as avg_risk_score,
    toUInt64(SUM(amount)) as total_volume_usd
FROM cryptobridge.transactions 
GROUP BY account_type
ORDER BY fraud_rate_percent DESC;

-- =================================================================
-- 💰 VISUALIZATION 3: Top Crypto Assets (Donut Chart)
-- =================================================================
-- TRANSACTION VOLUME BY CRYPTOCURRENCY - Perfect for Donut/Pie Chart
SELECT 
    crypto_currency as cryptocurrency,
    COUNT(*) as transaction_count,
    toUInt64(SUM(amount)) as volume_usd,
    ROUND(AVG(amount), 0) as avg_amount
FROM cryptobridge.transactions 
GROUP BY crypto_currency
ORDER BY volume_usd DESC
LIMIT 10;

-- =================================================================
-- 🌐 VISUALIZATION 4: Geographic Heatmap Data
-- =================================================================
-- COUNTRY-WISE TRANSACTION SUMMARY - Perfect for World Map/Heatmap
SELECT 
    sender_country as country,
    COUNT(*) as outgoing_transactions,
    toUInt64(SUM(amount)) as outgoing_volume
FROM cryptobridge.transactions 
GROUP BY sender_country
UNION ALL
SELECT 
    receiver_country as country,
    COUNT(*) as incoming_transactions,
    toUInt64(SUM(amount)) as incoming_volume
FROM cryptobridge.transactions 
GROUP BY receiver_country
ORDER BY outgoing_volume DESC;

-- =================================================================
-- ⏰ VISUALIZATION 5: Time Series - Transaction Trends (Line Chart)
-- =================================================================
-- DAILY TRANSACTION PATTERNS - Perfect for Time Series Line Chart
SELECT 
    toDate(timestamp) as transaction_date,
    COUNT(*) as daily_transactions,
    toUInt64(SUM(amount)) as daily_volume,
    SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as daily_suspicious,
    ROUND(AVG(risk_score), 1) as avg_daily_risk
FROM cryptobridge.transactions 
GROUP BY toDate(timestamp)
ORDER BY transaction_date DESC
LIMIT 30;

-- =================================================================
-- 👑 VISUALIZATION 6: Top Users Dashboard (Table with Conditional Formatting)
-- =================================================================
-- TOP USERS BY VOLUME - Perfect for Rich Table with Formatting
SELECT 
    ua.user_id,
    ua.account_type,
    ua.country,
    ua.kyc_level,
    COUNT(t.transaction_id) as transaction_count,
    toUInt64(SUM(t.amount)) as total_volume_usd,
    ROUND(AVG(t.risk_score), 1) as avg_risk_score,
    SUM(CASE WHEN t.is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_count,
    arrayStringConcat(ua.preferred_cryptos, ', ') as preferred_cryptos
FROM cryptobridge.user_accounts ua
LEFT JOIN cryptobridge.transactions t ON (ua.user_id = t.sender_user_id OR ua.user_id = t.receiver_user_id)
GROUP BY ua.user_id, ua.account_type, ua.country, ua.kyc_level, ua.preferred_cryptos
ORDER BY total_volume_usd DESC
LIMIT 20;

-- =================================================================
-- 📈 VISUALIZATION 7: Real-Time Crypto Prices (Price Tracker)
-- =================================================================
-- LIVE CRYPTO PERFORMANCE - Perfect for Number Cards + Table
SELECT 
    symbol,
    ROUND(price, 6) as current_price,
    ROUND(price_change_24h, 2) as change_24h_percent,
    source as exchange,
    timestamp as last_updated
FROM cryptobridge.crypto_prices cp
WHERE (symbol, timestamp) IN (
    SELECT symbol, MAX(timestamp) as max_timestamp
    FROM cryptobridge.crypto_prices 
    GROUP BY symbol
)
AND symbol IN ('BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT', 'SOLUSDT', 'DOGEUSDT', 'TRXUSDT', 'MATICUSDT', 'AVAXUSDT')
ORDER BY current_price DESC;

-- =================================================================
-- 🔥 VISUALIZATION 8: High-Risk Alert Dashboard (Alert Table)
-- =================================================================
-- REAL-TIME FRAUD ALERTS - Perfect for Alert Table with Red Highlighting
SELECT 
    sender_user_id as user_id,
    sender_country as country,
    COUNT(*) as recent_transactions,
    toUInt64(SUM(amount)) as total_volume,
    ROUND(AVG(risk_score), 1) as avg_risk_score,
    SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_count,
    MAX(timestamp) as last_transaction,
    arrayStringConcat(arrayDistinct(arrayConcat([crypto_currency])), ', ') as currencies_used
FROM cryptobridge.transactions 
WHERE timestamp >= NOW() - INTERVAL '7 DAY'
AND risk_score >= 70
GROUP BY sender_user_id, sender_country
HAVING COUNT(*) >= 2
ORDER BY avg_risk_score DESC, total_volume DESC
LIMIT 25;

-- =================================================================
-- 📊 VISUALIZATION 9: Executive KPI Summary (Number Cards)
-- =================================================================
-- EXECUTIVE DASHBOARD METRICS - Perfect for Big Number Cards
SELECT 'Total Transactions' as metric, toString(COUNT(*)) as value FROM cryptobridge.transactions
UNION ALL
SELECT 'Total Volume (USD)', CONCAT('$', toString(toUInt64(SUM(amount)))) FROM cryptobridge.transactions
UNION ALL
SELECT 'Cross-Border Volume', CONCAT('$', toString(toUInt64(SUM(amount)))) FROM cryptobridge.transactions WHERE sender_country != receiver_country
UNION ALL
SELECT 'Suspicious Transactions', toString(SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END)) FROM cryptobridge.transactions
UNION ALL
SELECT 'Active Users', toString(COUNT(*)) FROM cryptobridge.user_accounts
UNION ALL
SELECT 'Countries Served', toString(COUNT(DISTINCT country)) FROM cryptobridge.user_accounts
UNION ALL
SELECT 'Crypto Assets', toString(COUNT(DISTINCT crypto_currency)) FROM cryptobridge.transactions
UNION ALL
SELECT 'Average Risk Score', toString(ROUND(AVG(risk_score), 1)) FROM cryptobridge.transactions;

-- =================================================================
-- 🎯 VISUALIZATION 10: Risk vs Volume Scatter Plot
-- =================================================================
-- RISK-VOLUME CORRELATION - Perfect for Scatter Plot
SELECT 
    transaction_id,
    amount as volume_usd,
    risk_score,
    CASE 
        WHEN account_type = 'whale' THEN 'Whale'
        WHEN account_type = 'suspicious' THEN 'Suspicious'
        ELSE 'Regular'
    END as account_category,
    sender_country,
    crypto_currency
FROM cryptobridge.transactions 
WHERE amount > 1000  -- Focus on significant transactions
ORDER BY RAND()
LIMIT 1000;  -- Sample for performance