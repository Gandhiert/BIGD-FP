#!/bin/bash

echo "🎮 Starting Gaming Analytics Dashboard Stack..."

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install docker-compose first."
    exit 1
fi

# Start MinIO
echo "📦 Starting MinIO..."
docker-compose up -d

# Wait for MinIO to be ready
echo "⏳ Waiting for MinIO to be ready..."
sleep 10

# Check Python version and handle Python 3.13 distutils issue
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "🐍 Detected Python version: $PYTHON_VERSION"

if [[ "$PYTHON_VERSION" == "3.13" ]]; then
    echo "⚠️  Python 3.13 detected. Using conda environment to avoid distutils issues..."
    
    # Check if conda is available
    if command -v conda &> /dev/null; then
        source $(conda info --base)/etc/profile.d/conda.sh
        
        # Check if gaming_analytics environment already exists
        if conda env list | grep -q "gaming_analytics"; then
            echo "🔧 Activating existing conda environment 'gaming_analytics'..."
            conda activate gaming_analytics
        else
            echo "🔧 Creating new conda environment 'gaming_analytics' with Python 3.11..."
            conda create -n gaming_analytics python=3.11 -y
            conda activate gaming_analytics
        fi
    else
        echo "❌ Python 3.13 has distutils issues. Please install conda or use Python 3.11/3.12"
        echo "   Alternative: pip install setuptools"
        
        # Try installing setuptools as fallback
        echo "🔧 Trying to install setuptools..."
        pip install setuptools
    fi
else
    # Check if Python virtual environment exists
    if [ ! -d "venv" ]; then
        echo "🐍 Creating Python virtual environment..."
        python3 -m venv venv
    fi
    
    # Activate virtual environment
    echo "🔧 Activating virtual environment..."
    source venv/bin/activate
fi

# Install requirements with error handling
echo "📦 Installing Python dependencies..."
if ! pip install -r requirements.txt; then
    echo "❌ Failed to install dependencies. Trying alternative approach..."
    
    # Install critical packages one by one
    echo "🔧 Installing critical packages individually..."
    pip install pandas numpy flask streamlit plotly requests minio pyyaml
    
    if [ $? -ne 0 ]; then
        echo "❌ Failed to install dependencies. Please check your Python environment."
        exit 1
    fi
fi

# Start Analytics API in background
echo "🚀 Starting Analytics API..."
cd data_lake/scripts
python analytics_api.py &
API_PID=$!
cd ../..

# Wait for API to start
echo "⏳ Waiting for API to start..."
sleep 5

# Check if API is running
if ! curl -s http://localhost:5000/api/health > /dev/null; then
    echo "⚠️  API might not be running properly. Check logs if dashboard doesn't work."
fi

# Start Streamlit Dashboard
echo "📊 Starting Gaming Dashboard..."
streamlit run dashboard/gaming_dashboard.py \
  --server.port 8501 \
  --server.headless true \
  --server.enableCORS false \
  --server.enableXsrfProtection false  &
DASHBOARD_PID=$!

# Wait for Streamlit to start
echo "⏳ Waiting for Streamlit to start..."
sleep 10

echo ""
echo "✅ Gaming Analytics Dashboard Stack Started!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔗 Services:"
echo "   MinIO Console: http://localhost:9001"
echo "   Analytics API: http://localhost:5000"
echo "   Gaming Dashboard: http://localhost:8501"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "💡 To stop all services, press Ctrl+C"
echo "📝 If you see errors, try: pip install setuptools distutils-extra"

# Setup signal handler and keep running
trap "echo '🛑 Stopping services...'; kill $API_PID $DASHBOARD_PID 2>/dev/null; docker-compose down; exit" INT

# Keep script running without blocking
echo "🔄 Services are running. Press Ctrl+C to stop..."
while true; do
    sleep 5
    # Check if processes are still running
    if ! ps -p $API_PID > /dev/null 2>&1; then
        echo "⚠️  API process stopped unexpectedly"
    fi
    if ! ps -p $DASHBOARD_PID > /dev/null 2>&1; then
        echo "⚠️  Dashboard process stopped unexpectedly"
    fi
done 