#!/bin/bash
# Script to sync/import a downloaded database file
# Usage: ./sync_database.sh [path_to_downloaded_db]

DOWNLOADED_DB="${1}"
TARGET_DB="data/investors.db"
BACKUP_DIR="data/backups"

if [ -z "$DOWNLOADED_DB" ]; then
    # Look for the most recent downloaded database file
    DOWNLOADED_DB=$(ls -t ~/Downloads/investors_*.db 2>/dev/null | head -1)
    
    if [ -z "$DOWNLOADED_DB" ]; then
        echo "❌ No database file found!"
        echo ""
        echo "Usage: ./sync_database.sh [path_to_downloaded_db]"
        echo "   or place investors_*.db in ~/Downloads/"
        exit 1
    fi
fi

if [ ! -f "$DOWNLOADED_DB" ]; then
    echo "❌ Database file not found: $DOWNLOADED_DB"
    exit 1
fi

echo "🔄 Syncing database..."
echo "Source: $DOWNLOADED_DB"
echo "Target: $TARGET_DB"
echo ""

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Backup current database if it exists
if [ -f "$TARGET_DB" ]; then
    BACKUP_FILE="$BACKUP_DIR/investors_backup_$(date +%Y%m%d_%H%M%S).db"
    cp "$TARGET_DB" "$BACKUP_FILE"
    echo "✅ Backup created: $BACKUP_FILE"
fi

# Copy downloaded database to target location
cp "$DOWNLOADED_DB" "$TARGET_DB"
echo "✅ Database synced successfully!"
echo ""
echo "📊 Database info:"
python3 << EOF
import sqlite3
from pathlib import Path

db_path = "$TARGET_DB"
if Path(db_path).exists():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM investors")
    count = cursor.fetchone()[0]
    conn.close()
    print(f"   Total investors: {count}")
else:
    print("   Database not found")
EOF

echo ""
echo "🚀 You can now use the local database with your app!"

