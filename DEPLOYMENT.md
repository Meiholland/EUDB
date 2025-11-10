# Deployment Guide

## Why Vercel Doesn't Work

Vercel is designed for serverless functions and static sites (Next.js, React, etc.). Streamlit apps require a Python runtime environment, which Vercel doesn't support natively.

## Recommended Deployment Options

### Option 1: Streamlit Cloud (Easiest & Free) ⭐

**Best for**: Quick deployment, free hosting, zero configuration

1. Go to https://share.streamlit.io/
2. Sign in with your GitHub account
3. Click "New app"
4. Select repository: `Meiholland/EUDB`
5. Main file path: `src/app.py`
6. Click "Deploy"

Your app will be live at: `https://your-app-name.streamlit.app`

**Note**: Streamlit Cloud automatically:
- Detects `requirements.txt`
- Installs dependencies
- Deploys your app
- Provides a public URL

### Option 2: Render (Free Tier Available)

1. Go to https://render.com
2. Sign up/login with GitHub
3. Click "New +" → "Web Service"
4. Connect your GitHub repository: `Meiholland/EUDB`
5. Configure:
   - **Name**: investor-data-hub
   - **Environment**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `streamlit run src/app.py --server.port=$PORT --server.address=0.0.0.0`
6. Click "Create Web Service"

### Option 3: Railway

1. Go to https://railway.app
2. Sign up with GitHub
3. Click "New Project" → "Deploy from GitHub repo"
4. Select `Meiholland/EUDB`
5. Railway will auto-detect Python and install dependencies
6. Add start command: `streamlit run src/app.py --server.port=$PORT`

### Option 4: Heroku

1. Install Heroku CLI: https://devcenter.heroku.com/articles/heroku-cli
2. Create `Procfile`:
   ```
   web: streamlit run src/app.py --server.port=$PORT --server.address=0.0.0.0
   ```
3. Deploy:
   ```bash
   heroku create your-app-name
   git push heroku main
   ```

### Option 5: Fly.io

1. Install Fly CLI: https://fly.io/docs/getting-started/installing-flyctl/
2. Create `fly.toml` (Fly will generate this)
3. Deploy:
   ```bash
   fly launch
   fly deploy
   ```

## Important Notes

- **Database**: The SQLite database (`data/investors.db`) is stored locally. For production, consider:
  - Using a cloud database (PostgreSQL, Supabase)
  - Or using persistent storage volumes (Render, Railway support this)
  
- **File Uploads**: Uploaded files in `data/raw/` are stored locally. Consider:
  - Using cloud storage (S3, Google Cloud Storage)
  - Or using the platform's persistent storage

- **Environment Variables**: If you need secrets, use your platform's environment variable settings

## Quick Deploy to Streamlit Cloud

The easiest path:

1. Make sure your code is pushed to GitHub (✅ Done!)
2. Go to https://share.streamlit.io/
3. Click "New app"
4. Select `Meiholland/EUDB`
5. Main file: `src/app.py`
6. Deploy!

That's it! Your app will be live in minutes.

