# YouTube Analytics Dashboard

A Flask web application for analyzing YouTube videos using AI-powered insights. The dashboard displays script quality scores, conversion analysis, description CTR analysis, and affiliate product recommendations.

## Features

- **Dashboard Overview**: View KPI cards with total videos, analyzed videos, average scores, and revenue metrics
- **Video List**: Browse all videos with filtering and pagination
- **Video Detail Pages**: Comprehensive analysis results for individual videos including:
  - Script Quality Analysis (hook effectiveness, CTA score, persuasion techniques)
  - Affiliate Product Recommendations (AI-powered suggestions)
  - Description CTR Analysis (optimization suggestions)
  - Conversion Rate Analysis (revenue drivers)
- **Batch Analysis**: Run AI analysis on multiple videos at once
- **Analysis History**: Track all past analysis jobs

## Technology Stack

- **Backend**: Flask 3.0+ (Python web framework)
- **Database**: Google BigQuery (reads existing data, writes analysis results)
- **AI**: Claude API via Anthropic SDK
- **Frontend**: Bootstrap 5, Chart.js
- **Caching**: Flask-Caching (simple cache for development, Redis for production)

## Prerequisites

- Python 3.8+
- Google Cloud service account with BigQuery access
- Anthropic Claude API key
- Existing BigQuery tables with video data

## Installation

1. **Navigate to the web_app directory**:
   ```bash
   cd C:\Python\desktop_python_files\running_scripts\yt_analytics\ClaudeCode\web_app
   ```

2. **Create and activate virtual environment**:
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**:
   - Edit `.env.web` file with your credentials:
     - `GOOGLE_CREDENTIALS_PATH`: Path to BigQuery service account JSON
     - `BIGQUERY_PROJECT_ID`: Your Google Cloud project ID
     - `ANTHROPIC_API_KEY`: Your Claude API key

## Configuration

The application uses the following configuration files:

- **`.env.web`**: Environment variables (credentials, API keys)
- **`config.py`**: Flask configuration classes (development, production, testing)

Key configuration settings:
- `VIDEOS_PER_PAGE`: Number of videos per page (default: 25)
- `MAX_CONCURRENT_ANALYSES`: Max concurrent analyses (default: 5)
- `ANALYSIS_RATE_LIMIT_SECONDS`: Seconds between AI API calls (default: 2)

## BigQuery Tables

### Existing Tables (Read-Only)
The application reads from these existing BigQuery tables:

- `company-wide-370010.1_Youtube_Metrics_Dump.YT_Video_Registration_V2`
  - Video metadata (title, URL, description, published date)

- `company-wide-370010.Digibot.Daily_Rev_Metrics_by_Video_ID`
  - Revenue metrics (revenue, clicks, sales, views)

- `company-wide-370010.1_misc.YT_Transcript`
  - Video transcripts

- `company-wide-370010.1_YT_Serp_result.ALL_Time YT Serp`
  - SERP rankings

### Analysis Result Tables (Write)
The application writes analysis results to these existing tables:

- `company-wide-370010.1_Youtube_Metrics_Dump.AI_Analysis_Results`
  - Script quality scores and analysis

- `company-wide-370010.1_Youtube_Metrics_Dump.AI_Affiliate_Recommendations`
  - AI-generated product recommendations

- `company-wide-370010.1_Youtube_Metrics_Dump.AI_Description_Analysis`
  - Description CTR analysis

- `company-wide-370010.1_Youtube_Metrics_Dump.AI_Conversion_Analysis`
  - Conversion rate analysis and drivers

## Running the Application

### Development Mode

```bash
# Activate virtual environment
venv\Scripts\activate

# Run Flask development server
python app.py
```

The application will be available at: `http://localhost:5000`

### Production Mode

For production deployment, use a WSGI server like Gunicorn:

```bash
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

## Application Structure

```
web_app/
├── app/
│   ├── __init__.py              # Flask app factory
│   ├── models.py                # Data models
│   ├── extensions.py            # Flask extensions
│   ├── blueprints/              # Route blueprints
│   │   ├── dashboard.py         # Dashboard routes
│   │   ├── videos.py            # Video routes
│   │   ├── analysis.py          # Analysis routes
│   │   └── api.py               # API routes
│   ├── services/                # Business logic
│   │   ├── bigquery_service.py  # BigQuery operations
│   │   └── analysis_service.py  # AI analysis orchestration
│   ├── templates/               # HTML templates
│   │   ├── base.html
│   │   ├── dashboard/
│   │   ├── videos/
│   │   └── analysis/
│   └── static/                  # CSS, JS, images
│       ├── css/
│       └── js/
├── app.py                       # Application entry point
├── config.py                    # Configuration
├── requirements.txt             # Python dependencies
└── .env.web                     # Environment variables
```

## API Endpoints

### Web Routes

- `GET /` - Dashboard overview
- `GET /dashboard` - Dashboard with statistics
- `GET /dashboard/videos` - Video list with filters
- `GET /videos/<video_id>` - Video detail page
- `POST /videos/<video_id>/analyze` - Trigger single video analysis
- `GET /analysis/trigger` - Analysis trigger form
- `POST /analysis/trigger` - Execute analysis
- `GET /analysis/status/<job_id>` - Check analysis status
- `GET /analysis/history` - View analysis history

### API Routes (JSON)

- `GET /api/v1/videos` - List videos (JSON)
- `GET /api/v1/videos/<video_id>` - Video details (JSON)
- `GET /api/v1/analysis/<video_id>` - Latest analysis results (JSON)
- `GET /api/v1/dashboard/stats` - Dashboard statistics (JSON)
- `POST /api/v1/analysis/trigger` - Trigger analysis (JSON)

## Usage

1. **View Dashboard**: Navigate to `http://localhost:5000` to see the overview with KPI cards and recent videos

2. **Browse Videos**: Click "Videos" in the navigation to browse all videos with filtering options

3. **View Analysis**: Click on any video to see detailed analysis results

4. **Run Analysis**:
   - Click "Run Analysis" in navigation
   - Choose single video or batch mode
   - Select analysis types to run
   - Click "Start Analysis"

5. **Monitor Progress**: View analysis progress and results in the status page

## Analysis Types

The application supports four types of AI analysis:

1. **Script Quality Analysis**
   - Script quality score (1-10)
   - Hook effectiveness score
   - Call-to-action score
   - Persuasion effectiveness score
   - User intent match score
   - Identified persuasion techniques
   - Key strengths and improvement areas

2. **Description CTR Analysis**
   - Description quality score
   - CTA effectiveness score
   - SEO score
   - Link analysis (total links, affiliate links)
   - Optimization suggestions

3. **Affiliate Product Recommendations**
   - Top 5 recommended products
   - Relevance scores
   - Conversion probability
   - Recommendation reasoning
   - Where to mention in video

4. **Conversion Rate Analysis**
   - Conversion rate metrics
   - Revenue per click
   - Conversion drivers
   - Underperformance reasons
   - Actionable recommendations

## Troubleshooting

### BigQuery Connection Issues

- Verify service account JSON file path in `.env.web`
- Ensure service account has BigQuery read/write permissions
- Check project ID matches your Google Cloud project

### Analysis Failures

- Verify Anthropic API key is valid
- Check if video has transcript available
- Review rate limiting settings
- Check logs for detailed error messages

### Performance Issues

- Enable caching (Redis for production)
- Increase rate limit delay between API calls
- Reduce batch analysis size

## Development

### Adding New Analysis Types

1. Create analyzer class in `../src/analyzers/`
2. Import in `app/services/analysis_service.py`
3. Add to `analyze_video()` method
4. Update data models in `app/models.py`
5. Update BigQuery service to store results
6. Update templates to display results

### Running Tests

```bash
pytest tests/
```

## Contributing

1. Create a feature branch
2. Make your changes
3. Write/update tests
4. Submit a pull request

## License

Copyright 2026 - YouTube Analytics Dashboard

## Support

For issues or questions, please contact the development team.
