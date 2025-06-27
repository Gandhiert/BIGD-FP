#!/bin/bash

# Games Real-time Clustering System Launcher
# Menjalankan seluruh pipeline clustering games secara otomatis

set -e  # Exit on any error

# Colors untuk output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
KAFKA_SERVERS="localhost:9092"
MINIO_ENDPOINT="localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
API_ENDPOINT="http://localhost:5000"
CLUSTERING_INTERVAL=2  # minutes
MIN_GAMES_THRESHOLD=80

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_LAKE_DIR="${SCRIPT_DIR}/data_lake"
SCRIPTS_DIR="${DATA_LAKE_DIR}/scripts"
LOGS_DIR="${DATA_LAKE_DIR}/logs"

# Function untuk print dengan warna
print_header() {
    echo -e "${CYAN}======================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}======================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️ $1${NC}"
}

print_step() {
    echo -e "${PURPLE}🔄 $1${NC}"
}

# Function untuk check dependencies
check_dependencies() {
    print_step "Checking system dependencies..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python3 is required but not installed"
        exit 1
    fi
    print_success "Python3 found"
    
    # Check Java (for Spark)
    if ! command -v java &> /dev/null; then
        print_warning "Java not found. PySpark may not work properly"
    else
        print_success "Java found"
    fi
    
    # Check pip packages
    print_step "Checking Python packages..."
    if python3 -c "import kafka, minio, pandas, pyspark, flask" 2>/dev/null; then
        print_success "All required Python packages found"
    else
        print_error "Some required Python packages are missing"
        print_info "Installing required packages..."
        pip3 install -r requirements.txt || {
            print_error "Failed to install packages"
            exit 1
        }
    fi
    
    # Check games.csv
    if [[ ! -f "${SCRIPT_DIR}/games.csv" ]]; then
        print_error "games.csv not found in project root"
        print_info "Please ensure games.csv is in the project root directory"
        exit 1
    fi
    print_success "games.csv found"
    
    # Check if Kafka is running
    print_step "Checking Kafka connectivity..."
    if python3 -c "
from kafka import KafkaProducer
try:
    producer = KafkaProducer(bootstrap_servers='${KAFKA_SERVERS}', request_timeout_ms=5000)
    producer.close()
    print('Kafka OK')
except:
    print('Kafka FAIL')
" | grep -q "Kafka OK"; then
        print_success "Kafka is accessible"
    else
        print_error "Cannot connect to Kafka at ${KAFKA_SERVERS}"
        print_info "Please ensure Kafka is running"
        exit 1
    fi
    
    # Check if MinIO is running
    print_step "Checking MinIO connectivity..."
    if python3 -c "
from minio import Minio
try:
    client = Minio('${MINIO_ENDPOINT}', access_key='${MINIO_ACCESS_KEY}', secret_key='${MINIO_SECRET_KEY}', secure=False)
    list(client.list_buckets())
    print('MinIO OK')
except:
    print('MinIO FAIL')
" | grep -q "MinIO OK"; then
        print_success "MinIO is accessible"
    else
        print_error "Cannot connect to MinIO at ${MINIO_ENDPOINT}"
        print_info "Please ensure MinIO is running"
        exit 1
    fi
}

# Function untuk create directories
setup_directories() {
    print_step "Setting up directories..."
    
    # Create logs directory
    mkdir -p "${LOGS_DIR}"
    print_success "Logs directory created: ${LOGS_DIR}"
    
    # Create necessary MinIO buckets
    print_step "Creating MinIO buckets..."
    python3 << EOF
from minio import Minio

client = Minio('${MINIO_ENDPOINT}', access_key='${MINIO_ACCESS_KEY}', secret_key='${MINIO_SECRET_KEY}', secure=False)

buckets = ['streaming-zone', 'clusters-zone', 'warehouse-zone']
for bucket in buckets:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")
    else:
        print(f"Bucket exists: {bucket}")
EOF
    print_success "MinIO buckets ready"
}

# Function untuk show system status
show_system_status() {
    print_header "GAMES CLUSTERING SYSTEM STATUS"
    
    echo -e "${BLUE}📊 Configuration:${NC}"
    echo "   Kafka Servers: ${KAFKA_SERVERS}"
    echo "   MinIO Endpoint: ${MINIO_ENDPOINT}"
    echo "   API Endpoint: ${API_ENDPOINT}"
    echo "   Clustering Interval: ${CLUSTERING_INTERVAL} minutes"
    echo "   Min Games Threshold: ${MIN_GAMES_THRESHOLD}"
    echo ""
    
    echo -e "${BLUE}📁 File Locations:${NC}"
    echo "   Project Root: ${SCRIPT_DIR}"
    echo "   Data Lake: ${DATA_LAKE_DIR}"
    echo "   Scripts: ${SCRIPTS_DIR}"
    echo "   Logs: ${LOGS_DIR}"
    echo "   Games CSV: ${SCRIPT_DIR}/games.csv"
    echo ""
    
    echo -e "${BLUE}🎯 System Components:${NC}"
    echo "   🏭 Kafka Producer: Games CSV streaming"
    echo "   🔄 Kafka Consumer: Stream to MinIO"
    echo "   ⚡ PySpark Job: K-Means clustering (k=13)"
    echo "   🌐 Analytics API: Cluster data endpoints"
    echo "   🎪 Orchestrator: End-to-end automation"
    echo ""
}

# Function untuk start individual components
start_producer() {
    print_step "Starting Kafka Producer..."
    cd "${SCRIPTS_DIR}"
    python3 games_csv_producer.py \
        --csv-file "../../games.csv" \
        --kafka-servers "${KAFKA_SERVERS}" \
        --topic "gaming.games.stream" \
        --batch-size 50 \
        --delay 2.0 \
        --shuffle \
        --loop &
    echo $! > "${LOGS_DIR}/producer.pid"
    print_success "Producer started (PID: $(cat ${LOGS_DIR}/producer.pid))"
}

start_consumer() {
    print_step "Starting Kafka Consumer..."
    cd "${SCRIPTS_DIR}"
    python3 kafka_consumer.py \
        --bootstrap-servers "${KAFKA_SERVERS}" \
        --minio-endpoint "${MINIO_ENDPOINT}" \
        --buffer-size 100 \
        --flush-interval 30 &
    echo $! > "${LOGS_DIR}/consumer.pid"
    print_success "Consumer started (PID: $(cat ${LOGS_DIR}/consumer.pid))"
}

start_api() {
    print_step "Starting Analytics API..."
    cd "${SCRIPTS_DIR}"
    python3 analytics_api.py &
    echo $! > "${LOGS_DIR}/api.pid"
    print_success "API started (PID: $(cat ${LOGS_DIR}/api.pid))"
}

start_orchestrator() {
    print_step "Starting Clustering Orchestrator..."
    cd "${SCRIPTS_DIR}"
    python3 clustering_orchestrator.py \
        --kafka-servers "${KAFKA_SERVERS}" \
        --minio-endpoint "${MINIO_ENDPOINT}" \
        --minio-access-key "${MINIO_ACCESS_KEY}" \
        --minio-secret-key "${MINIO_SECRET_KEY}" \
        --api-endpoint "${API_ENDPOINT}" \
        --clustering-interval "${CLUSTERING_INTERVAL}" \
        --min-games "${MIN_GAMES_THRESHOLD}"
}

# Function untuk stop all components
stop_all() {
    print_step "Stopping all components..."
    
    # Stop processes by PID
    for pid_file in "${LOGS_DIR}"/*.pid; do
        if [[ -f "$pid_file" ]]; then
            pid=$(cat "$pid_file")
            component=$(basename "$pid_file" .pid)
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid"
                print_success "Stopped $component (PID: $pid)"
            fi
            rm -f "$pid_file"
        fi
    done
    
    # Kill any remaining processes
    pkill -f "games_csv_producer.py" 2>/dev/null || true
    pkill -f "kafka_consumer.py" 2>/dev/null || true
    pkill -f "analytics_api.py" 2>/dev/null || true
    pkill -f "clustering_orchestrator.py" 2>/dev/null || true
    
    print_success "All components stopped"
}

# Function untuk show logs
show_logs() {
    if [[ -d "${LOGS_DIR}" ]]; then
        print_info "Recent log files in ${LOGS_DIR}:"
        ls -la "${LOGS_DIR}"/*.log 2>/dev/null || print_warning "No log files found"
    else
        print_warning "Logs directory not found"
    fi
}

# Function untuk run clustering manually
run_clustering_manual() {
    print_step "Running clustering job manually..."
    cd "${SCRIPTS_DIR}"
    python3 ../spark_jobs/games_clustering.py \
        --minio-endpoint "${MINIO_ENDPOINT}" \
        --minio-access-key "${MINIO_ACCESS_KEY}" \
        --minio-secret-key "${MINIO_SECRET_KEY}" \
        --input-bucket "streaming-zone" \
        --output-bucket "clusters-zone" \
        --k-clusters 13 \
        --use-csv
    
    if [[ $? -eq 0 ]]; then
        print_success "Clustering job completed successfully"
    else
        print_error "Clustering job failed"
        exit 1
    fi
}

# Function untuk test API endpoints
test_api() {
    print_step "Testing API endpoints..."
    
    # Test health endpoint
    if curl -s "${API_ENDPOINT}/api/health" > /dev/null; then
        print_success "Health endpoint responsive"
    else
        print_error "Health endpoint not responsive"
        return 1
    fi
    
    # Test clustering endpoints
    print_info "Testing clustering endpoints..."
    echo "Clustering Status:"
    curl -s "${API_ENDPOINT}/api/clustering/status" | python3 -m json.tool || print_warning "Clustering status endpoint failed"
    
    echo -e "\nCluster Analysis:"
    curl -s "${API_ENDPOINT}/api/clustering/analysis" | head -n 20 || print_warning "Cluster analysis endpoint failed"
    
    echo -e "\nSample Clustered Games:"
    curl -s "${API_ENDPOINT}/api/clustering/games?limit=5" | python3 -m json.tool || print_warning "Clustered games endpoint failed"
}

# Main function
main() {
    print_header "🎮 GAMES REAL-TIME CLUSTERING SYSTEM"
    
    case "${1:-}" in
        "start")
            show_system_status
            check_dependencies
            setup_directories
            
            print_header "STARTING SYSTEM COMPONENTS"
            start_producer
            sleep 2
            start_consumer
            sleep 3
            start_api
            
            # Wait longer for API to be ready before starting orchestrator
            print_info "Waiting for API to be fully ready..."
            sleep 15
            
            # Test API connectivity before starting orchestrator
            print_step "Testing API connectivity..."
            if curl -s --max-time 30 "${API_ENDPOINT}/api/health" > /dev/null; then
                print_success "API is ready!"
            else
                print_warning "API not immediately ready, orchestrator will handle retries..."
            fi
            
            print_success "All components started successfully!"
            print_info "Starting orchestrator (this will run continuously)..."
            start_orchestrator
            ;;
            
        "stop")
            stop_all
            ;;
            
        "status")
            show_system_status
            print_header "COMPONENT STATUS"
            
            # Check running processes
            if pgrep -f "games_csv_producer.py" > /dev/null; then
                print_success "Producer is running"
            else
                print_warning "Producer is not running"
            fi
            
            if pgrep -f "kafka_consumer.py" > /dev/null; then
                print_success "Consumer is running"
            else
                print_warning "Consumer is not running"
            fi
            
            if pgrep -f "analytics_api.py" > /dev/null; then
                print_success "API is running"
            else
                print_warning "API is not running"
            fi
            
            if pgrep -f "clustering_orchestrator.py" > /dev/null; then
                print_success "Orchestrator is running"
            else
                print_warning "Orchestrator is not running"
            fi
            ;;
            
        "logs")
            show_logs
            if [[ -n "${2:-}" ]]; then
                log_file="${LOGS_DIR}/${2}.log"
                if [[ -f "$log_file" ]]; then
                    print_info "Showing last 50 lines of $log_file:"
                    tail -n 50 "$log_file"
                else
                    print_error "Log file not found: $log_file"
                fi
            fi
            ;;
            
        "cluster")
            run_clustering_manual
            ;;
            
        "test")
            test_api
            ;;
            
        "quick-start")
            show_system_status
            check_dependencies
            setup_directories
            
            print_header "QUICK START - RUNNING CLUSTERING ON CSV"
            run_clustering_manual
            
            print_step "Starting API for testing..."
            start_api
            sleep 5
            
            test_api
            
            print_success "Quick start completed! API is running at ${API_ENDPOINT}"
            print_info "Try these endpoints:"
            echo "  ${API_ENDPOINT}/api/clustering/status"
            echo "  ${API_ENDPOINT}/api/clustering/games"
            echo "  ${API_ENDPOINT}/api/clustering/analysis"
            echo "  ${API_ENDPOINT}/api/clustering/recommendations?game_id=13500"
            ;;
            
        *)
            print_header "USAGE"
            echo "Usage: $0 [COMMAND]"
            echo ""
            echo "Commands:"
            echo "  start        - Start full real-time clustering system"
            echo "  stop         - Stop all components"
            echo "  status       - Show system status"
            echo "  logs [name]  - Show logs (optional: specify component name)"
            echo "  cluster      - Run clustering manually on games.csv"
            echo "  test         - Test API endpoints"
            echo "  quick-start  - Quick demo: cluster + API testing"
            echo ""
            echo "Examples:"
            echo "  $0 quick-start    # Demo clustering and API"
            echo "  $0 start          # Full real-time system"
            echo "  $0 logs producer  # Show producer logs"
            echo "  $0 cluster        # Manual clustering"
            echo "  $0 test           # Test API endpoints"
            echo ""
            exit 1
            ;;
    esac
}

# Trap untuk cleanup on exit
trap 'stop_all' EXIT INT TERM

# Run main function
main "$@" 