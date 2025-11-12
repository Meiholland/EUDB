#!/bin/bash
# Script to manually commit and push the database
# Usage: ./commit_database.sh [commit message]

COMMIT_MSG="${1:-Update investors database}"

echo "📦 Adding database to Git..."
git add -f data/investors.db

echo "💾 Committing database..."
git commit -m "$COMMIT_MSG"

echo "🚀 Pushing to GitHub..."
git push origin main

echo "✅ Database committed and pushed!"

