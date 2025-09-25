#!/bin/bash

# 🚀 CryptoBridge Complete Data Pipeline Startup
# Enterprise Medallion Architeecho "🎭 Data Lake Architecture:"
echo "   🥉 Bronze Layer:  Raw data ingestion (Kafka to HDFS)"
echo "   🥈 Silver Layer:  Data cleaning & enrichment"
echo "   🥇 Gold Layer:    Business analytics & KPIs"e with Real-Time Streaming & Batch Processing

echo "🎭 Starting CryptoBridge Complete Data Pipeline"
echo "==============================================="
echo "🏗️ Medallion Architecture: Bronze → Silver → Gold"
echo "⚡ Real-Time Streaming + Batch Processing"
echo "📊 Enterprise Analytics & Business Intelligence"
echo "==============================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start Docker Compose services (including new data lake services)
echo "📦 Starting all Docker services..."
echo "   🔄 Kafka + Zookeeper (Streaming)"
echo "   🗄️ HDFS (Distributed Storage)"
echo "   ⚡ Spark (Processing Engine)"
echo "   📊 ClickHouse (Analytics Database)"
docker compose up -d

# Wait for services to be ready (extended time for HDFS)
echo "⏳ Waiting for services to initialize..."
echo "   📍 Core services: 30 seconds"
sleep 30

echo "   📍 HDFS NameNode initialization: 20 seconds"
sleep 20

echo "   📍 Spark cluster formation: 15 seconds"
sleep 15

# Check critical services
echo "🔍 Checking critical services..."

# Check HDFS NameNode
if curl -s http://localhost:9870 > /dev/null; then
    echo "   ✅ HDFS NameNode: Ready"
else
    echo "   ⚠️ HDFS NameNode: Starting up..."
fi

# Check Spark Master
if curl -s http://localhost:8080 > /dev/null; then
    echo "   ✅ Spark Master: Ready"
else
    echo "   ⚠️ Spark Master: Starting up..."
fi

# Check Kafka
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "   ✅ Kafka: Ready"
else
    echo "   ⚠️ Kafka: Starting up..."
fi

# Setup Kafka topics
echo "📡 Setting up Kafka topics..."
python3 scripts/setup_kafka_topics.py

# Setup HDFS directories for data lake
echo "🗂️ Setting up HDFS directory structure..."
echo "   📁 Creating Bronze layer directories..."
docker exec namenode hdfs dfs -mkdir -p /crypto-bridge-datalake/bronze
docker exec namenode hdfs dfs -mkdir -p /crypto-bridge-datalake/silver  
docker exec namenode hdfs dfs -mkdir -p /crypto-bridge-datalake/gold
docker exec namenode hdfs dfs -mkdir -p /crypto-bridge-datalake/checkpoints

echo "   📁 Setting permissions..."
docker exec namenode hdfs dfs -chmod -R 777 /crypto-bridge-datalake

echo "   ✅ Data lake directory structure ready"

# Show comprehensive service status
echo "📊 Complete Service Status:"
echo "=========================================="
docker compose ps

echo ""
echo "🎯 Pipeline Setup Complete!"
echo "=========================================="
echo "🔗 Access Points:"
echo "   📡 Kafka UI:      http://localhost:8090"
echo "   ⚡ Spark Master:  http://localhost:8080"
echo "   🗄️ HDFS NameNode: http://localhost:9870"
echo "   📊 ClickHouse:    http://localhost:8123"
echo ""
echo "� Data Lake Architecture:"
echo "   🥉 Bronze Layer:  Raw data ingestion (Kafka → HDFS)"
echo "   🥈 Silver Layer:  Data cleaning & enrichment"
echo "   🥇 Gold Layer:    Business analytics & KPIs"
echo ""
echo "🚀 Quick Start Options:"
echo "=========================================="
echo "   Option A - Complete Medallion Pipeline:"
echo "     python3 scripts/data_lake_orchestrator.py"
echo ""
echo "   Option B - Individual Components:"
echo "     1. Producer:  python3 scripts/kafka_transaction_producer.py"
echo "     2. Consumer:  python3 scripts/kafka_transaction_consumer.py"
echo "     3. Bronze:    python3 scripts/bronze_layer_processor.py"
echo "     4. Silver:    python3 scripts/silver_layer_processor.py"
echo "     5. Gold:      python3 scripts/gold_layer_processor.py"
echo ""
echo "📊 Monitoring & Management:"
echo "   • Monitor data lake files: http://localhost:9870"
echo "   • Track Spark jobs: http://localhost:8080"
echo "   • View Kafka topics: http://localhost:8090"
echo "   • Query analytics: http://localhost:8123"
echo ""
echo "🛑 To stop all services: docker compose down"