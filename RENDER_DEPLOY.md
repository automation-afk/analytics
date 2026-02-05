# Deploy to Render (Free) - Quick Guide

## Step 1: Prepare Your Code

Make sure you have a `render.yaml` file (optional but recommended):

```yaml
services:
  - type: web
    name: yt-analytics-dashboard
    env: python
    region: oregon
    plan: free
    buildCommand: pip install -r requirements.txt
    startCommand: gunicorn -b :$PORT "app:create_app('production')"
    envVars:
      - key: FLASK_SECRET_KEY
        generateValue: true
      - key: BIGQUERY_PROJECT_ID
        value: company-wide-370010
      - key: CACHE_TYPE
        value: simple
      - key: LOG_LEVEL
        value: INFO
```

## Step 2: Push to GitHub

```bash
cd c:\Python\desktop_python_files\running_scripts\yt_analytics\ClaudeCode\web_app

# Initialize git if not already
git init
git add .
git commit -m "Initial commit"

# Create GitHub repo and push
git remote add origin https://github.com/YOUR_USERNAME/yt-analytics-dashboard.git
git push -u origin main
```

## Step 3: Deploy on Render

1. Go to https://render.com
2. Sign up with GitHub
3. Click **"New +"** → **"Web Service"**
4. Select your repository
5. Configure:
   - **Name**: `yt-analytics-dashboard`
   - **Region**: Oregon (Free)
   - **Branch**: main
   - **Runtime**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn -b :$PORT "app:create_app('production')"`
   - **Instance Type**: Free

6. Add Environment Variables:
   - `FLASK_SECRET_KEY`: (click "Generate" for random value)
   - `BIGQUERY_PROJECT_ID`: `company-wide-370010`
   - `ANTHROPIC_API_KEY`: (your Anthropic API key)
   - `CACHE_TYPE`: `simple`
   - `LOG_LEVEL`: `INFO`

7. Click **"Create Web Service"**

## Step 4: Handle Google Credentials

Since you need BigQuery access, you have two options:

### Option A: Use Environment Variable (Recommended)
1. Copy your credentials JSON content
2. In Render, add environment variable:
   - `GOOGLE_APPLICATION_CREDENTIALS_JSON`: (paste entire JSON)
3. Update your code to load from env var instead of file

### Option B: Upload as File
1. Use Render Disk (paid feature)
2. Or rebuild with credentials baked in (not recommended for security)

## Your Site Will Be Live At:
`https://yt-analytics-dashboard.onrender.com`

## Important Notes:
- **Free tier spins down after 15 minutes** of inactivity
- First request after spin-down takes ~30 seconds
- Upgrade to paid ($7/month) to keep it always running
- Free tier includes:
  - 750 hours/month (enough for 24/7)
  - 512MB RAM
  - 0.1 CPU
  - Free SSL

## Logs & Monitoring:
- View logs in Render dashboard
- Check build/deploy status
- Monitor crashes and errors
