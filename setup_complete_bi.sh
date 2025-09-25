#!/bin/bash

# 🚀 CryptoBridge Complete Setup & Business Intelligence Pipeline
# Generates fake data, integrates real APIs, and sets up Metabase BI

echo "🚀 Starting CryptoBridge Complete Business Intelligence Setup"
echo "=============================================================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed. Please install Python 3 first."
        exit 1
    fi
    
    # Check pip
    if ! command -v pip &> /dev/null; then
        if ! command -v pip3 &> /dev/null; then
            print_error "pip is not installed. Please install pip first."
            exit 1
        fi
    fi
    
    print_status "All prerequisites are met!"
}

# Install Python dependencies
install_dependencies() {
    print_info "Installing Python dependencies..."
    
    # Update pip first
    python3 -m pip install --upgrade pip
    
    # Install requirements
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
        print_status "Python dependencies installed successfully!"
    else
        print_error "requirements.txt not found!"
        exit 1
    fi
}

# Start infrastructure services
start_infrastructure() {
    print_info "Starting infrastructure services..."
    
    # Stop any existing services
    docker-compose down -v
    
    # Start core services (Kafka, ClickHouse, Metabase)
    docker-compose up -d zookeeper kafka clickhouse metabase-db metabase
    
    print_info "Waiting for services to initialize..."
    sleep 30
    
    # Check if ClickHouse is ready
    print_info "Waiting for ClickHouse to be ready..."
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s "http://localhost:8123/ping" > /dev/null 2>&1; then
            print_status "ClickHouse is ready!"
            break
        fi
        
        attempt=$((attempt + 1))
        if [ $attempt -eq $max_attempts ]; then
            print_error "ClickHouse failed to start within expected time"
            exit 1
        fi
        
        print_info "Waiting for ClickHouse... (attempt $attempt/$max_attempts)"
        sleep 5
    done
    
    # Check if Metabase is ready
    print_info "Waiting for Metabase to be ready..."
    max_attempts=60  # Metabase takes longer to start
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s "http://localhost:3000/api/health" > /dev/null 2>&1; then
            print_status "Metabase is ready!"
            break
        fi
        
        attempt=$((attempt + 1))
        if [ $attempt -eq $max_attempts ]; then
            print_warning "Metabase is taking longer to start. You can check manually at http://localhost:3000"
            break
        fi
        
        if [ $((attempt % 10)) -eq 0 ]; then
            print_info "Waiting for Metabase... (attempt $attempt/$max_attempts)"
        fi
        sleep 5
    done
    
    print_status "Core infrastructure services are running!"
}

# Generate comprehensive fake data and fetch real API data
generate_data() {
    print_info "Generating comprehensive business data..."
    
    cd scripts
    
    # Run the advanced data generator
    if [ -f "advanced_data_generator.py" ]; then
        python3 advanced_data_generator.py
        if [ $? -eq 0 ]; then
            print_status "Data generation completed successfully!"
        else
            print_error "Data generation failed!"
            exit 1
        fi
    else
        print_error "advanced_data_generator.py not found!"
        exit 1
    fi
    
    cd ..
}

# Load data into ClickHouse
load_data_to_clickhouse() {
    print_info "Loading data into ClickHouse..."
    
    cd scripts
    
    # Load data using the enhanced data loader
    if [ -f "clickhouse_data_loader.py" ]; then
        python3 clickhouse_data_loader.py
        if [ $? -eq 0 ]; then
            print_status "Data loaded into ClickHouse successfully!"
        else
            print_error "Data loading failed!"
            exit 1
        fi
    else
        print_error "clickhouse_data_loader.py not found!"
        exit 1
    fi
    
    cd ..
}

# Set up Kafka topics and start producers
setup_kafka_streaming() {
    print_info "Setting up Kafka topics and streaming..."
    
    cd scripts
    
    # Set up Kafka topics
    if [ -f "setup_kafka_topics.py" ]; then
        python3 setup_kafka_topics.py
        print_status "Kafka topics created!"
    fi
    
    # Start Kafka producer in background
    if [ -f "kafka_transaction_producer.py" ]; then
        print_info "Starting Kafka transaction producer..."
        nohup python3 kafka_transaction_producer.py > ../logs/producer.log 2>&1 &
        PRODUCER_PID=$!
        echo $PRODUCER_PID > ../logs/producer.pid
        print_status "Kafka producer started (PID: $PRODUCER_PID)"
    fi
    
    cd ..
}

# Set up Metabase BI dashboards
setup_metabase_bi() {
    print_info "Setting up Metabase Business Intelligence dashboards..."
    
    # Wait for Metabase to fully initialize
    sleep 30
    
    cd scripts
    
    if [ -f "metabase_setup.py" ]; then
        # Note: This requires manual Metabase setup first
        print_info "Metabase BI setup script is ready."
        print_warning "Please complete Metabase initial setup at http://localhost:3000 first"
        print_info "Then run: python3 scripts/metabase_setup.py"
    fi
    
    cd ..
}

# Create monitoring and status dashboard
create_monitoring_dashboard() {
    print_info "Creating monitoring dashboard..."
    
    # Create logs directory
    mkdir -p logs
    
    # Create a simple status checker
    cat > check_services.sh << 'EOF'
#!/bin/bash

echo "🔍 CryptoBridge Services Status Check"
echo "====================================="

# Check ClickHouse
if curl -s "http://localhost:8123/ping" > /dev/null 2>&1; then
    echo "✅ ClickHouse: Running (Port 8123)"
else
    echo "❌ ClickHouse: Not accessible"
fi

# Check Kafka
if nc -z localhost 9092 2>/dev/null; then
    echo "✅ Kafka: Running (Port 9092)"
else
    echo "❌ Kafka: Not accessible"
fi

# Check Metabase
if curl -s "http://localhost:3000/api/health" > /dev/null 2>&1; then
    echo "✅ Metabase: Running (Port 3000)"
else
    echo "❌ Metabase: Not accessible"
fi

# Check Kafka UI
if curl -s "http://localhost:8090" > /dev/null 2>&1; then
    echo "✅ Kafka UI: Running (Port 8090)"
else
    echo "❌ Kafka UI: Not accessible"
fi

echo ""
echo "📊 Quick ClickHouse Data Check:"
curl -s "http://localhost:8123/?user=analytics&password=analytics123&query=SELECT COUNT(*) as transaction_count FROM cryptobridge.transactions FORMAT JSON" | python3 -m json.tool 2>/dev/null || echo "No data or connection issue"

echo ""
echo "🔗 Access URLs:"
echo "   📊 Metabase BI:     http://localhost:3000"
echo "   🗄️ ClickHouse:      http://localhost:8123 (analytics/analytics123)"
echo "   📡 Kafka UI:        http://localhost:8090"
echo "   📈 Spark Master:    http://localhost:8080"
EOF

    chmod +x check_services.sh
    print_status "Service monitoring dashboard created (run ./check_services.sh)"
}

# Main execution flow
main() {
    print_info "Starting CryptoBridge Complete Setup..."
    
    # Step 1: Prerequisites
    check_prerequisites
    
    # Step 2: Install dependencies
    install_dependencies
    
    # Step 3: Start infrastructure
    start_infrastructure
    
    # Step 4: Generate data
    generate_data
    
    # Step 5: Load data into ClickHouse
    load_data_to_clickhouse
    
    # Step 6: Set up Kafka streaming
    setup_kafka_streaming
    
    # Step 7: Create monitoring tools
    create_monitoring_dashboard
    
    # Step 8: Display completion status
    echo ""
    echo "🎉 CRYPTOBRIDGE BUSINESS INTELLIGENCE SETUP COMPLETE!"
    echo "=============================================================="
    echo ""
    print_status "Infrastructure Services:"
    echo "   🗄️ ClickHouse Analytics:    http://localhost:8123"
    echo "   📊 Metabase BI Platform:    http://localhost:3000"
    echo "   📡 Kafka Streaming:         http://localhost:8090"
    echo "   📈 Spark Processing:        http://localhost:8080"
    echo ""
    print_status "Business Intelligence Features:"
    echo "   ✅ Cross-border Transfer Analysis"
    echo "   ✅ Top Account Volume Tracking"
    echo "   ✅ Real-time Crypto Price Trends"
    echo "   ✅ Exchange Volume Comparison"
    echo "   ✅ Advanced Fraud Detection"
    echo "   ✅ Time-windowed Transaction Analysis"
    echo ""
    print_info "Next Steps:"
    echo "1. 📊 Complete Metabase setup at http://localhost:3000"
    echo "2. 🔧 Run: python3 scripts/metabase_setup.py"
    echo "3. 📈 Monitor services: ./check_services.sh"
    echo "4. 📋 View business analytics in Metabase dashboards"
    echo ""
    print_warning "Note: Metabase initial setup requires manual configuration"
    echo "Default login will be created during first access."
    echo ""
    
    # Keep producer running message
    if [ -f "logs/producer.pid" ]; then
        PRODUCER_PID=$(cat logs/producer.pid)
        print_info "Kafka producer is running in background (PID: $PRODUCER_PID)"
        print_info "To stop producer: kill $PRODUCER_PID"
    fi
    
    # Run final service check
    echo ""
    ./check_services.sh
}

# Execute main function
main "$@"