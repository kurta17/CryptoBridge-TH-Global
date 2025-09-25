# 🌉 CryptoBridge-TH-Global

A comprehensive **Kafka-based streaming platform** for analyzing cryptocurrency transactions between Binance Global and Binance Thailand, featuring real-time streaming, fraud detection, and business intelligence.

## 🎯 Overview

CryptoBridge provides:

- **Real-time transaction streaming** using Apache Kafka
- **Fraud detection pipeline** with configurable rules
- **Multi-tier data processing** (Bronze/Silver/Gold layers)
- **Spark Streaming** for high-throughput processing
- **HDFS integration** for distributed storage
- **Docker orchestration** for easy deployment

## 🏗️ Streaming Architecture

```
📡 Producer → Kafka Topics → 🏠 ClickHouse → 📊 HDFS Medallion Architecture
     ↓              ↓              ↓              ↓
Fake Data    Topic Routing   Real-time        Bronze → Silver → Gold
Generation   & Partitioning  Analytics         Data Lake Layers
```

### 🔄 Workflow Process

1. **Data Generation**: `kafka_transaction_producer.py` generates realistic transaction data (3 tx/sec)
2. **Stream Processing**: Kafka topics route transactions by risk level
3. **Real-time Analytics**: `clickhouse_consumer.py` ingests data into ClickHouse for real-time queries
4. **Medallion Architecture**: Bronze/Silver/Gold layer processors transform raw data into analytics-ready format
5. **Storage**: HDFS stores processed data across Bronze → Silver → Gold layers
6. **Business Intelligence**: ClickHouse enables fast analytical queries on transaction data

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- 16GB RAM recommended

### 1. Start the Pipeline

```bash
# Clone and navigate
git clone <repo-url>
cd CryptoBridge-TH-Global

# Start all services
./start_pipeline.sh
```

### 2. Access Monitoring

- **Kafka UI**: http://localhost:8090
- **Spark UI**: http://localhost:8080
- **HDFS NameNode**: http://localhost:9870
- **ClickHouse**: http://localhost:8123

### 3. Run Data Pipeline

```bash
# Option A - Complete Medallion Pipeline:
python3 scripts/data_lake_orchestrator.py

# Option B - Individual Components:
# Terminal 1: Start Producer
python3 scripts/kafka_transaction_producer.py

# Terminal 2: Start ClickHouse Consumer
python3 scripts/clickhouse_consumer.py

# Terminal 3: Process Data Layers
python3 scripts/bronze_layer_processor.py
python3 scripts/silver_layer_processor.py  
python3 scripts/gold_layer_processor.py
```

## 📡 Kafka Topics Structure

| Topic                         | Purpose              | Partitions |
| ----------------------------- | -------------------- | ---------- |
| `transactions-normal`       | Regular transactions | 6          |
| `transactions-suspicious`   | Flagged transactions | 3          |
| `transactions-fraud-alerts` | Real-time alerts     | 2          |
| `analytics-aggregations`    | Processed metrics    | 4          |

## 🏠 ClickHouse Analytics Features

- **Real-time Ingestion** from Kafka streams
- **High-performance Analytics** with columnar storage
- **Time-series Analysis** with partition by month
- **Fraud Detection Queries** with sub-second response
- **Business Intelligence** ready data structure

## 🔥 Spark Streaming Features

- **Structured Streaming** with Kafka integration
- **Watermarking** for late data handling
- **Fraud Detection** with configurable rules
- **Multi-layer Processing** (Bronze→Silver→Gold)
- **Checkpointing** for fault tolerance

## 📊 Data Layers

### Bronze Layer (Raw)

- Direct Kafka ingestion
- JSON message format
- Timestamp-based partitioning

### Silver Layer (Cleaned)

- Data validation & cleansing
- Schema enforcement
- Fraud scoring

### Gold Layer (Analytics)

- Aggregated metrics
- Business KPIs
- ML-ready features

## 🛡️ Fraud Detection Rules

- **High Amount Transactions**: >$50,000
- **Suspicious Patterns**: Multiple rapid transfers
- **Velocity Checks**: Transaction frequency limits
- **Geo-location Analysis**: Cross-border patterns
- **Account Behavior**: Deviation from normal patterns

## 🐳 Docker Services

| Service       | Port | Description         |
| ------------- | ---- | ------------------- |
| Kafka         | 9092 | Message broker      |
| Zookeeper     | 2181 | Kafka coordination  |
| Kafka UI      | 8090 | Topic monitoring    |
| Spark Master  | 8080 | Cluster management  |
| Spark Worker  | 8081 | Processing nodes    |
| HDFS NameNode | 9870 | Distributed storage |
| ClickHouse    | 8123 | Analytics database  |

## 📂 Project Structure

```
CryptoBridge-TH-Global/
├── 📡 scripts/
│   ├── kafka_transaction_producer.py    # Real-time data generation
│   ├── kafka_transaction_consumer.py    # Spark streaming processor
│   ├── clickhouse_consumer.py          # ClickHouse analytics ingestion
│   ├── bronze_layer_processor.py       # Bronze layer processing
│   ├── silver_layer_processor.py       # Silver layer processing
│   ├── gold_layer_processor.py         # Gold layer processing
│   ├── data_lake_orchestrator.py       # Complete pipeline orchestration
│   └── setup_kafka_topics.py           # Topic initialization
├── 📊 data/
│   ├── raw/                             # Source data
│   ├── bronze/                          # Raw ingested data
│   ├── clean/                           # Processed JSON
│   ├── gold/                            # Analytics-ready data
│   └── schemas/                         # Data schemas
├── 🔧 docker-compose.yml               # Infrastructure setup
├── 🚀 start_pipeline.sh                # Startup orchestration
└── 📋 requirements.txt                 # Python dependencies
```

## 🔧 Configuration

### Producer Settings

```python
STREAMING_CONFIG = {
    'transactions_per_second': 3.0,
    'fraud_percentage': 15.0,
    'burst_mode': True,
    'topics': {
        'normal': 'transactions-normal',
        'suspicious': 'transactions-suspicious'
    }
}
```

### Consumer Settings

```python
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 9000,
    'database': 'cryptobridge',
    'user': 'analytics',
    'password': 'analytics123'
}
```

## 📈 Analytics Capabilities

- **Real-time Dashboards**: Transaction volume, fraud rates
- **Risk Scoring**: ML-based transaction risk assessment
- **Pattern Detection**: Anomaly identification
- **Cross-Exchange Analysis**: Binance Global ↔ TH flows
- **Regulatory Reporting**: AML compliance metrics

## 🛑 Stopping Services 

```bash
# Stop all services
docker-compose down

# Stop with volume cleanup
docker-compose down -v
```

## 🔍 Troubleshooting

### Common Issues

1. **Memory Issues**: Increase Docker memory to 16GB+
2. **Port Conflicts**: Check ports 8080, 8090, 9092 availability
3. **Kafka Connection**: Wait 30s after `docker-compose up`
4. **Topic Creation**: Run `setup_kafka_topics.py` manually

### Monitoring Commands

```bash
# Check service status
docker-compose ps

# View Kafka topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Monitor consumer lag
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group streaming-processor

# Check ClickHouse connection
docker-compose exec clickhouse clickhouse-client --database cryptobridge --user analytics --password analytics123

# View HDFS directories
docker-compose exec namenode hdfs dfs -ls /
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Add tests for streaming components
4. Submit pull request

## 📄 License

MIT License - See LICENSE file for details
