# 📊 CryptoBridge Business Intelligence - Visualization Setup Guide

## 🎯 **Step-by-Step Metabase Dashboard Creation**

### 🚀 **Quick Setup Instructions**
1. **Access Metabase**: http://localhost:3000
2. **Navigate to**: New → Question → Native Query
3. **Select Database**: Your ClickHouse connection
4. **Copy & Paste** each query below
5. **Choose Visualization Type** as recommended
6. **Save & Add to Dashboard**

---

## 📈 **Visualization 1: Cross-Border Money Flow**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 1
**🎨 Chart Type**: **Horizontal Bar Chart**
**⚙️ Settings**: 
- X-axis: `volume_usd`
- Y-axis: `corridor`
- Color by: `transactions`
**💡 Expected Result**: UK→TH leads with $369M volume

---

## 🚨 **Visualization 2: Fraud Detection Dashboard**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 2  
**🎨 Chart Type**: **Grouped Bar Chart**
**⚙️ Settings**:
- X-axis: `account_type`
- Y-axis: `fraud_rate_percent` and `total_transactions`
- Multiple series enabled
**💡 Expected Result**: Suspicious accounts = 100% fraud rate, Whales = 4.4%, Regular = 0.2%

---

## 💰 **Visualization 3: Crypto Assets Distribution**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 3
**🎨 Chart Type**: **Donut Chart** or **Pie Chart**
**⚙️ Settings**:
- Dimension: `cryptocurrency`
- Metric: `volume_usd`
**💡 Expected Result**: Shows volume distribution across BTC, ETH, USDT, etc.

---

## 🌍 **Visualization 4: Global Transaction Heatmap**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 4
**🎨 Chart Type**: **World Map** or **Table**
**⚙️ Settings**:
- Region: `country`
- Metric: `outgoing_volume`
**💡 Expected Result**: Thailand and major economies highlighted

---

## ⏰ **Visualization 5: Transaction Trends Over Time**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 5
**🎨 Chart Type**: **Line Chart**
**⚙️ Settings**:
- X-axis: `transaction_date`
- Y-axis: `daily_volume` and `daily_transactions`
- Multiple series for volume and count
**💡 Expected Result**: Daily transaction patterns and volume trends

---

## 👑 **Visualization 6: Top Users Dashboard**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 6
**🎨 Chart Type**: **Table**
**⚙️ Settings**:
- Conditional formatting on `avg_risk_score` (red > 50)
- Column widths optimized
- Sort by `total_volume_usd`
**💡 Expected Result**: Top 20 users with whale accounts leading

---

## 📈 **Visualization 7: Live Crypto Prices**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 7
**🎨 Chart Type**: **Table** with **Number Cards**
**⚙️ Settings**:
- Format `current_price` as currency
- Color `change_24h_percent` (green positive, red negative)
**💡 Expected Result**: Real-time crypto prices from Binance

---

## 🔥 **Visualization 8: High-Risk Alerts**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 8
**🎨 Chart Type**: **Alert Table**
**⚙️ Settings**:
- Red highlighting for `avg_risk_score` > 80
- Sort by risk score descending
- Auto-refresh every 5 minutes
**💡 Expected Result**: Real-time fraud monitoring alerts

---

## 📊 **Visualization 9: Executive KPIs**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 9
**🎨 Chart Type**: **Number Cards**
**⚙️ Settings**:
- Large font size
- Grid layout (2x4)
- Color coding for different metric types
**💡 Expected Result**: 
- 60,155+ Total Transactions
- $3.9B+ Total Volume
- 375 Active Users
- Multiple countries served

---

## 🎯 **Visualization 10: Risk vs Volume Analysis**
**📋 Query**: Copy from `metabase_visualizations.sql` - Visualization 10
**🎨 Chart Type**: **Scatter Plot**
**⚙️ Settings**:
- X-axis: `volume_usd` (log scale)
- Y-axis: `risk_score`
- Color by: `account_category`
- Size by: `volume_usd`
**💡 Expected Result**: Clear clustering of suspicious vs normal transactions

---

## 🎨 **Dashboard Assembly Tips**

### 📐 **Layout Recommendations**:
```
[Executive KPIs - 4 number cards across top]
[Cross-Border Flow Chart] [Fraud Detection Chart]
[Crypto Performance Table] [Geographic Heatmap]
[Transaction Trends - Full Width Line Chart]
[Top Users Table - Full Width]
[Risk Alerts Table - Full Width]
```

### 🎨 **Styling Tips**:
- **Colors**: Use red for fraud/risk, green for normal, blue for volume
- **Refresh**: Set auto-refresh to 15 minutes for operational dashboards
- **Filters**: Add date range filters for time-based analysis
- **Responsive**: Test on different screen sizes

### ⚡ **Performance Tips**:
- Use `LIMIT` clauses for large result sets
- Add date filters to reduce data scanning
- Consider materialized views for complex aggregations

---

## 🎉 **Expected Dashboard Results**

Your completed dashboard will show:
- **$3.9B+ transaction volume** across 60K+ transactions
- **UK→TH corridor dominance** ($369M volume)
- **Clear fraud patterns**: Suspicious accounts vs regular users
- **375 active users** across multiple countries
- **Real-time crypto prices** from Binance APIs
- **Geographic distribution** of transactions
- **Risk-based alerts** for fraud detection

## 🔄 **Next Steps**
1. Create all 10 visualizations using the provided queries
2. Assemble into a master dashboard
3. Set up auto-refresh and filters
4. Share with stakeholders
5. Create additional specialized dashboards for specific use cases