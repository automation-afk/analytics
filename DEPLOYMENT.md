# Deployment Guide - YouTube Analytics Dashboard

## Option 1: Google Cloud Run (Recommended)

### Prerequisites
1. Google Cloud account with billing enabled
2. Google Cloud SDK installed: https://cloud.google.com/sdk/docs/install
3. Docker installed (optional, Cloud Build will handle this)

### Step-by-Step Deployment

#### 1. Set Up Google Cloud Project

```bash
# Login to Google Cloud
gcloud auth login

# Set your project ID (use your existing project)
gcloud config set project company-wide-370010

# Enable required APIs
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable containerregistry.googleapis.com
```

#### 2. Prepare Environment Variables

Create a file called `.env.production` with your production settings:

```bash
FLASK_SECRET_KEY=your-super-secret-key-generate-a-strong-one
BIGQUERY_PROJECT_ID=company-wide-370010
ANTHROPIC_API_KEY=your-anthropic-api-key
CACHE_TYPE=simple
LOG_LEVEL=INFO
```

**IMPORTANT**: Generate a strong SECRET_KEY:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

#### 3. Upload Google Credentials to Cloud

Your service account credentials need to be accessible. Two options:

**Option A: Use Application Default Credentials (Recommended)**
```bash
# Cloud Run will automatically use the service account of the project
# Make sure your Cloud Run service has BigQuery access
```

**Option B: Upload credentials as Secret**
```bash
# Create a secret for the credentials
gcloud secrets create bigquery-credentials \
    --data-file="C:\Python\desktop_python_files\running_scripts\yt_analytics\ClaudeCode\credentials\company_wide_cred.json"
```

#### 4. Deploy to Cloud Run

**Method 1: Direct Deploy (Easiest)**

```bash
# Navigate to project directory
cd c:\Python\desktop_python_files\running_scripts\yt_analytics\ClaudeCode\web_app

# Deploy directly with gcloud
gcloud run deploy yt-analytics-dashboard \
  --source . \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 300 \
  --max-instances 10 \
  --set-env-vars="FLASK_SECRET_KEY=your-secret-key-here,BIGQUERY_PROJECT_ID=company-wide-370010,ANTHROPIC_API_KEY=your-key,CACHE_TYPE=simple,LOG_LEVEL=INFO"
```

**Method 2: Using Cloud Build**

```bash
# Submit build to Cloud Build
gcloud builds submit --config cloudbuild.yaml .
```

#### 5. Set Up Custom Domain (Optional)

```bash
# Map custom domain
gcloud run domain-mappings create \
  --service yt-analytics-dashboard \
  --domain your-domain.com \
  --region us-central1
```

#### 6. Configure Service Account Permissions

```bash
# Get your Cloud Run service account
SERVICE_ACCOUNT=$(gcloud run services describe yt-analytics-dashboard \
  --region us-central1 \
  --format="value(spec.template.spec.serviceAccountName)")

# Grant BigQuery access
gcloud projects add-iam-policy-binding company-wide-370010 \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding company-wide-370010 \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.jobUser"
```

### Your Website Will Be Live At:
`https://yt-analytics-dashboard-[random-hash]-uc.a.run.app`

---

## Option 2: Google App Engine

### Deployment Steps

#### 1. Create app.yaml

```yaml
runtime: python311
entrypoint: gunicorn -b :$PORT "app:create_app('production')"

instance_class: F4

env_variables:
  FLASK_SECRET_KEY: "your-secret-key"
  BIGQUERY_PROJECT_ID: "company-wide-370010"
  ANTHROPIC_API_KEY: "your-key"
  CACHE_TYPE: "simple"

automatic_scaling:
  target_cpu_utilization: 0.65
  min_instances: 1
  max_instances: 10
```

#### 2. Deploy

```bash
gcloud app deploy
```

---

## Option 3: Other Platforms

### Railway (Simple Alternative)

1. Sign up at https://railway.app
2. Connect your GitHub repository
3. Set environment variables in Railway dashboard
4. Deploy automatically on push

### Heroku

```bash
# Install Heroku CLI
# Create Procfile:
echo "web: gunicorn -b :$PORT 'app:create_app(\"production\")'" > Procfile

# Deploy
heroku create yt-analytics-dashboard
heroku config:set FLASK_SECRET_KEY=your-key
heroku config:set ANTHROPIC_API_KEY=your-key
git push heroku main
```

---

## Important Production Checklist

### Security
- [ ] Generate strong SECRET_KEY (use `secrets.token_hex(32)`)
- [ ] Set `DEBUG = False` in production config
- [ ] Use HTTPS only (Cloud Run provides this automatically)
- [ ] Keep .env files out of version control (.gitignore)
- [ ] Secure API keys and credentials

### Performance
- [ ] Enable Redis cache for production (optional but recommended)
- [ ] Set appropriate memory/CPU limits
- [ ] Configure auto-scaling parameters
- [ ] Enable Cloud CDN for static files (optional)

### Database
- [ ] SQLite works for local analysis storage, but consider:
  - Cloud SQL for PostgreSQL (if scaling heavily)
  - Cloud Firestore (for NoSQL)
  - Current setup stores in BigQuery, so SQLite is fine for cache

### Monitoring
- [ ] Enable Cloud Logging
- [ ] Set up error reporting
- [ ] Configure uptime checks
- [ ] Set up alerts for errors/downtime

### Environment Variables Needed

```bash
FLASK_SECRET_KEY=<generate-strong-random-key>
BIGQUERY_PROJECT_ID=company-wide-370010
GOOGLE_CREDENTIALS_PATH=/app/credentials/company_wide_cred.json  # or use default credentials
ANTHROPIC_API_KEY=<your-anthropic-api-key>
CACHE_TYPE=simple  # or 'redis' with REDIS_URL
LOG_LEVEL=INFO
```

---

## Testing Your Deployment

1. Visit your Cloud Run URL
2. Test login with authorized email
3. Check dashboard loads correctly
4. Try triggering an analysis
5. Monitor logs for errors:

```bash
gcloud run services logs read yt-analytics-dashboard --region us-central1
```

---

## Costs Estimate (Google Cloud Run)

- **Free tier**: 2 million requests/month, 360,000 GB-seconds/month
- **After free tier**: ~$0.00002400 per request
- **Estimate**: For moderate usage (10,000 requests/month), expect **< $5/month**
- **BigQuery**: Charged separately based on data processed

---

## Troubleshooting

### Issue: "Permission denied" errors
**Solution**: Ensure service account has BigQuery permissions

### Issue: "Memory exceeded"
**Solution**: Increase memory in deployment command (--memory 4Gi)

### Issue: "Cold start slow"
**Solution**: Set min-instances to 1 to keep warm:
```bash
gcloud run services update yt-analytics-dashboard \
  --min-instances 1 --region us-central1
```

### Issue: Analysis timing out
**Solution**: Increase timeout:
```bash
gcloud run services update yt-analytics-dashboard \
  --timeout 900 --region us-central1
```

---

## Quick Deploy Command (All-in-One)

```bash
cd c:\Python\desktop_python_files\running_scripts\yt_analytics\ClaudeCode\web_app

gcloud run deploy yt-analytics-dashboard \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --timeout 300 \
  --set-env-vars="FLASK_SECRET_KEY=$(python -c 'import secrets; print(secrets.token_hex(32))'),BIGQUERY_PROJECT_ID=company-wide-370010,ANTHROPIC_API_KEY=YOUR_KEY_HERE,CACHE_TYPE=simple"
```

Replace `YOUR_KEY_HERE` with your actual Anthropic API key.

Your website will be live in 2-3 minutes!
