#!/bin/bash

echo "🚀 Starting Simple Gaming Analytics Stack"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Function untuk colored output
print_status() {
    echo -e "\033[1;32m[INFO]\033[0m $1"
}

print_warning() {
    echo -e "\033[1;33m[WARN]\033[0m $1"
}

print_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
}

# Check prerequisites
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

print_status "Checking prerequisites..."

if ! command_exists docker-compose; then
    print_error "docker-compose not found. Please install docker-compose first."
    exit 1
fi

if ! command_exists python3; then
    print_error "python3 not found. Please install Python 3.8+ first."
    exit 1
fi

# Setup Python environment
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
print_status "Detected Python version: $PYTHON_VERSION"

if [[ "$PYTHON_VERSION" == "3.13" ]]; then
    print_warning "Python 3.13 detected. Using conda environment..."
    
    if command_exists conda; then
        source $(conda info --base)/etc/profile.d/conda.sh
        
        if conda env list | grep -q "gaming_analytics"; then
            print_status "Activating existing conda environment 'gaming_analytics'..."
            conda activate gaming_analytics
        else
            print_status "Creating new conda environment 'gaming_analytics' with Python 3.11..."
            conda create -n gaming_analytics python=3.11 -y
            conda activate gaming_analytics
        fi
    else
        print_error "Python 3.13 requires conda. Please install conda or use Python 3.11/3.12"
        exit 1
    fi
else
    if [ ! -d "venv" ]; then
        print_status "Creating Python virtual environment..."
        python3 -m venv venv
    fi
    
    print_status "Activating virtual environment..."
    source venv/bin/activate
fi

# Install required dependencies
print_status "Installing required dependencies..."
pip install -q pandas numpy flask streamlit plotly requests minio pyyaml schedule \
    sqlalchemy textblob vaderSentiment kafka-python confluent-kafka

# Start infrastructure (MinIO + Kafka only)
print_status "Starting infrastructure (MinIO + Kafka)..."
docker-compose up -d

print_status "Waiting for services to start..."
sleep 30

# Check service health
print_status "Checking service health..."

# Check MinIO
MAX_RETRIES=10
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:9000/minio/health/live > /dev/null; then
        print_status "✅ MinIO is healthy"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        print_warning "⚠️ MinIO not ready yet (attempt $RETRY_COUNT/$MAX_RETRIES)..."
        sleep 5
    fi
done

# Check Kafka
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        print_status "✅ Kafka is healthy"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        print_warning "⚠️ Kafka not ready yet (attempt $RETRY_COUNT/$MAX_RETRIES)..."
        sleep 5
    fi
done

# Create MinIO buckets
print_status "Creating MinIO buckets..."
python3 -c "
import os
import sys
from minio import Minio
from minio.error import S3Error
import time

# Wait for MinIO to be fully ready
time.sleep(5)

# Initialize MinIO client
client = Minio(
    'localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

# Buckets to create
buckets = ['raw-zone', 'warehouse-zone', 'streaming-zone']

for bucket_name in buckets:
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f'✅ Created bucket: {bucket_name}')
        else:
            print(f'✅ Bucket already exists: {bucket_name}')
    except S3Error as e:
        print(f'❌ Error creating bucket {bucket_name}: {e}')
        
print('🗄️ MinIO buckets setup completed')
"

# Create Kafka topics
print_status "Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic gaming.player.events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic gaming.reviews.new --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic gaming.server.logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic gaming.player.stats --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

print_status "✅ Kafka topics created successfully"

# Create necessary directories
print_status "Creating necessary directories..."
mkdir -p data_lake/raw data_lake/warehouse data_lake/logs

# ==============================================================
# STEP 1: Generate data jika raw dan warehouse kosong
# ==============================================================
print_status "📊 Step 1: Checking if data generation is needed..."

if [ ! -f "data_lake/raw/unstructured_reviews.txt" ] || [ ! -f "data_lake/warehouse/games.parquet" ]; then
    print_status "📈 Generating gaming data (raw and warehouse are empty)..."
    python steam_data_generator.py
    print_status "✅ Gaming data generated successfully"
else
    print_status "✅ Data already exists, skipping generation"
fi

cd data_lake/scripts

# ==============================================================
# STEP 2: Upload data raw dan warehouse ke MinIO
# ==============================================================
print_status "📤 Step 2: Uploading data to MinIO..."

# Upload raw data jika belum ada di MinIO
print_status "Uploading raw data to MinIO..."
python upload_to_minio.py > ../logs/upload_raw.log 2>&1
if [ $? -eq 0 ]; then
    print_status "✅ Raw data uploaded to MinIO successfully"
else
    print_warning "⚠️ Raw data upload had issues. Check logs."
fi

# Jika warehouse data belum ada, jalankan ETL
if [ ! -f "../warehouse/games.parquet" ]; then
    print_status "Running ETL pipeline to create warehouse data..."
    python download_from_minio_and_transform.py > ../logs/etl_transform.log 2>&1
    print_status "✅ ETL transformation completed"
fi

# Upload warehouse data
print_status "Uploading warehouse data to MinIO..."
python upload_warehouse_to_minio.py > ../logs/upload_warehouse.log 2>&1
if [ $? -eq 0 ]; then
    print_status "✅ Warehouse data uploaded to MinIO successfully"
else
    print_warning "⚠️ Warehouse data upload had issues. Check logs."
fi