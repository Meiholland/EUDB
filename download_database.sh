#!/bin/bash
# Script to download database from deployed Streamlit app
# Usage: ./download_database.sh [streamlit_app_url]

STREAMLIT_URL="${1:-https://your-app-name.streamlit.app}"
DB_PATH="data/investors.db"

echo "📥 Downloading database from Streamlit app..."
echo "URL: $STREAMLIT_URL"
echo ""

# Create data directory if it doesn't exist
mkdir -p data

# Try to download using curl
# Note: This requires the app to have a download endpoint or direct file access
# For Streamlit Cloud, you'll need to use the download button in the app UI
# This script is a placeholder - you'll need to manually download from the app

echo "⚠️  Note: Streamlit Cloud doesn't allow direct file downloads via URL."
echo ""
echo "📋 To download the database:"
echo "   1. Go to your Streamlit app: $STREAMLIT_URL"
echo "   2. Navigate to Settings tab"
echo "   3. Click 'Download Database (.db)' button"
echo "   4. Save the file as: $DB_PATH"
echo ""
echo "Or use this command after downloading:"
echo "   mv ~/Downloads/investors_*.db $DB_PATH"
echo ""
echo "Then run: ./sync_database.sh to replace your local database"

