#!/bin/bash

echo "🏗️ Starting Complete Data Lakehouse Stack..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Function untuk check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

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
print_status "Checking prerequisites..."

if ! command_exists docker-compose; then
    print_error "docker-compose not found. Please install docker-compose first."
    exit 1
fi

if ! command_exists python3; then
    print_error "python3 not found. Please install Python 3.8+ first."
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
print_status "Detected Python version: $PYTHON_VERSION"

# Setup Python environment
if [[ "$PYTHON_VERSION" == "3.13" ]]; then
    print_warning "Python 3.13 detected. Using conda environment to avoid distutils issues..."
    
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
        print_error "Python 3.13 has distutils issues. Please install conda or use Python 3.11/3.12"
        print_status "Alternative: pip install setuptools"
        pip install setuptools
    fi
else
    if [ ! -d "venv" ]; then
        print_status "Creating Python virtual environment..."
        python3 -m venv venv
    fi
    
    print_status "Activating virtual environment..."
    source venv/bin/activate
fi

# Install dependencies
print_status "Installing Python dependencies..."
if ! pip install -r requirements.txt; then
    print_warning "Failed to install all dependencies. Installing critical packages individually..."
    
    critical_packages=(
        "pandas" "numpy" "flask" "streamlit" "plotly" "requests" 
        "minio" "pyyaml" "schedule" "sqlalchemy" "textblob" "vaderSentiment"
    )
    
    for package in "${critical_packages[@]}"; do
        print_status "Installing $package..."
        pip install "$package"
    done
fi

# Start infrastructure
print_status "Starting MinIO infrastructure..."
docker-compose up -d

print_status "Waiting for MinIO to be ready..."
sleep 15

# Check MinIO health
if curl -s http://localhost:9000/minio/health/live > /dev/null; then
    print_status "✅ MinIO is healthy"
else
    print_warning "⚠️ MinIO might not be fully ready yet"
fi

# Initialize data governance
print_status "Initializing Data Governance..."
cd data_lake/scripts

# Create necessary directories
mkdir -p ../logs ../config

# Initialize data catalog and discover existing datasets
print_status "Setting up data catalog and governance..."
python data_governance.py discover > ../logs/governance_init.log 2>&1 &

# Start orchestrator in background
print_status "Starting Data Orchestrator..."
python data_orchestrator.py run-etl full > ../logs/initial_etl.log 2>&1
if [ $? -eq 0 ]; then
    print_status "✅ Initial ETL completed successfully"
else
    print_warning "⚠️ Initial ETL had issues. Check logs."
fi

# Start continuous orchestrator in background
nohup python data_orchestrator.py > ../logs/orchestrator.log 2>&1 &
ORCHESTRATOR_PID=$!
print_status "🎯 Data Orchestrator started (PID: $ORCHESTRATOR_PID)"

# Start Analytics API
print_status "Starting Advanced Analytics API..."
python analytics_api.py &
API_PID=$!

cd ../..

# Wait for API to be ready
print_status "Waiting for Analytics API to start..."
sleep 10

# Health check API
if curl -s http://localhost:5000/api/health > /dev/null; then
    print_status "✅ Analytics API is healthy"
else
    print_warning "⚠️ Analytics API might not be ready. Check logs if dashboard doesn't work."
fi

# Start Enhanced Dashboard
print_status "Starting Gaming Analytics Dashboard..."
streamlit run dashboard/gaming_dashboard.py \
  --server.port 8501 \
  --server.headless true \
  --server.enableCORS false \
  --server.enableXsrfProtection false &
DASHBOARD_PID=$!

# Wait for dashboard to start
print_status "Waiting for Dashboard to initialize..."
sleep 15

# Display summary
echo ""
echo "🎉 Complete Data Lakehouse Stack Successfully Started!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📋 Service Status:"
echo "   🗄️  MinIO Console:         http://localhost:9001"
echo "   🔧 Analytics API:          http://localhost:5000"
echo "   📊 Gaming Dashboard:       http://localhost:8501"
echo "   🎯 Data Orchestrator:      Running (PID: $ORCHESTRATOR_PID)"
echo ""
echo "🔧 Available Management Commands:"
echo "   Data Quality Check:        cd data_lake/scripts && python data_orchestrator.py quality-check"
echo "   Health Check:              cd data_lake/scripts && python data_orchestrator.py health-check"
echo "   Search Data Catalog:       cd data_lake/scripts && python data_governance.py search"
echo "   Data Lineage:              cd data_lake/scripts && python data_governance.py lineage <dataset>"
echo "   Governance Audit:          cd data_lake/scripts && python data_governance.py audit"
echo ""
echo "📁 Log Files:"
echo "   Orchestrator Logs:         data_lake/logs/orchestrator.log"
echo "   Quality Reports:           data_lake/logs/quality_report_*.json"
echo "   Governance Audits:         data_lake/logs/governance_audit_*.json"
echo "   Job History:               data_lake/logs/job_history.json"
echo ""
echo "🔑 Default MinIO Credentials:"
echo "   Username: minioadmin"
echo "   Password: minioadmin"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "💡 Features Available:"
echo "   ✅ Automated ETL Pipeline with Scheduling"
echo "   ✅ Advanced NLP Sentiment Analysis"  
echo "   ✅ Data Quality Monitoring"
echo "   ✅ Data Governance & Metadata Catalog"
echo "   ✅ Data Lineage Tracking"
echo "   ✅ Health Monitoring & Alerting"
echo "   ✅ Compliance & Security Auditing"
echo "   ✅ Interactive Analytics Dashboard"
echo ""
echo "🛑 To stop all services, press Ctrl+C"
echo ""

# Setup signal handler untuk graceful shutdown
cleanup() {
    echo ""
    print_status "🛑 Shutting down Data Lakehouse Stack..."
    
    # Kill background processes
    if [ ! -z "$ORCHESTRATOR_PID" ]; then
        kill $ORCHESTRATOR_PID 2>/dev/null
        print_status "📊 Stopped Data Orchestrator"
    fi
    
    if [ ! -z "$API_PID" ]; then
        kill $API_PID 2>/dev/null
        print_status "🔧 Stopped Analytics API"
    fi
    
    if [ ! -z "$DASHBOARD_PID" ]; then
        kill $DASHBOARD_PID 2>/dev/null
        print_status "📊 Stopped Dashboard"
    fi
    
    # Stop Docker containers
    docker-compose down
    print_status "🗄️ Stopped MinIO"
    
    print_status "✅ All services stopped successfully"
    exit 0
}

# Trap signals untuk cleanup
trap cleanup INT TERM

# Keep main process running dan monitor health
echo "🔄 Data Lakehouse is running. Monitoring system health..."
echo "   Use 'docker-compose logs minio' to see MinIO logs"
echo "   Use 'tail -f data_lake/logs/orchestrator.log' to see orchestrator logs"
echo ""

# Health monitoring loop
HEALTH_CHECK_INTERVAL=300  # 5 minutes
LAST_HEALTH_CHECK=0

while true; do
    sleep 30
    
    CURRENT_TIME=$(date +%s)
    
    # Periodic health checks
    if (( CURRENT_TIME - LAST_HEALTH_CHECK >= HEALTH_CHECK_INTERVAL )); then
        print_status "🏥 Running periodic health check..."
        
        # Check if processes are still running
        if ! ps -p $API_PID > /dev/null 2>&1; then
            print_warning "⚠️ Analytics API process stopped unexpectedly"
        fi
        
        if ! ps -p $DASHBOARD_PID > /dev/null 2>&1; then
            print_warning "⚠️ Dashboard process stopped unexpectedly"
        fi
        
        if ! ps -p $ORCHESTRATOR_PID > /dev/null 2>&1; then
            print_warning "⚠️ Orchestrator process stopped unexpectedly"
        fi
        
        # Check MinIO health
        if ! curl -s http://localhost:9000/minio/health/live > /dev/null; then
            print_warning "⚠️ MinIO health check failed"
        fi
        
        # Check API health
        if ! curl -s http://localhost:5000/api/health > /dev/null; then
            print_warning "⚠️ Analytics API health check failed"
        fi
        
        LAST_HEALTH_CHECK=$CURRENT_TIME
        print_status "✅ Health check completed"
    fi
done 