#!/bin/bash
# Quick start script for Investor Data Hub

echo "🚀 Starting Investor Data Hub..."
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip3 install -r requirements.txt

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data/raw logs

# Initialize database
echo "🗄️  Initializing database..."
python3 -c "from src.database import init_database; init_database()"

# Run Streamlit app
echo "🌐 Starting Streamlit app..."
echo ""
echo "✅ App will open in your browser at http://localhost:8501"
echo ""
python3 -m streamlit run src/app.py

