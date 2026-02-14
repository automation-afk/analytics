"""Flask application factory."""
import logging
import os
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix
from config import config
from app.extensions import cache


def create_app(config_name='development'):
    """
    Create and configure Flask application.

    Args:
        config_name: Configuration name (development, production, testing)

    Returns:
        Flask application instance
    """
    app = Flask(__name__)

    # Load configuration
    app.config.from_object(config[config_name])

    # Fix for running behind reverse proxy (Railway, Render, etc.)
    # This ensures Flask generates https:// URLs when behind a proxy
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    # Initialize extensions
    cache.init_app(app)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, app.config['LOG_LEVEL']),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Initialize Local Database service first (for storing/reading analysis results)
    from app.services.local_db_service import LocalDBService
    app.local_db = LocalDBService()

    # Initialize BigQuery service (for reading video data from BigQuery)
    from app.services.bigquery_service import BigQueryService
    app.bigquery = BigQueryService(
        credentials_path=app.config['GOOGLE_CREDENTIALS_PATH'],
        project_id=app.config['BIGQUERY_PROJECT_ID'],
        local_db=app.local_db  # Pass local_db for analysis results
    )

    # Initialize analytics service
    from app.services.analytics_service import AnalyticsService
    app.activity_logger = AnalyticsService(
        credentials_path=app.config['GOOGLE_CREDENTIALS_PATH']
    )

    # Initialize YouTube Comments service
    from app.services.youtube_comments_service import YouTubeCommentsService
    app.youtube_comments = YouTubeCommentsService(
        api_key=app.config.get('YOUTUBE_API_KEY'),
        credentials_path=app.config.get('YOUTUBE_CREDENTIALS_PATH'),
        local_db=app.local_db
    )

    # Load known affiliate brands for comment brand detection
    try:
        affiliates = app.bigquery.get_all_affiliates()
        if affiliates:
            app.youtube_comments.set_known_brands(affiliates)
            logging.info(f"Loaded {len(affiliates)} affiliate brands for comment detection")
    except Exception as e:
        logging.warning(f"Could not load affiliate brands: {e}")

    # Seed approved_brands table from PREFERRED_BRANDS config (if table is empty)
    try:
        existing_brands = app.local_db.get_approved_brands()
        if not existing_brands:
            preferred = app.config.get('PREFERRED_BRANDS', {})
            for silo, brand in preferred.items():
                app.local_db.store_approved_brand(silo=silo, primary_brand=brand)
            if preferred:
                logging.info(f"Seeded {len(preferred)} approved brands from config")
    except Exception as e:
        logging.warning(f"Could not seed approved brands: {e}")

    # Initialize OAuth
    from app.blueprints.auth import init_oauth
    init_oauth(app)

    # Register blueprints
    from app.blueprints import auth, dashboard, videos, analysis, api
    app.register_blueprint(auth.bp)
    app.register_blueprint(dashboard.bp)
    app.register_blueprint(videos.bp)
    app.register_blueprint(analysis.bp)
    app.register_blueprint(api.bp, url_prefix='/api/v1')

    # Register error handlers
    @app.errorhandler(404)
    def not_found(error):
        return {"error": "Not found"}, 404

    @app.errorhandler(500)
    def internal_error(error):
        return {"error": "Internal server error"}, 500

    # Add template filters
    @app.template_filter('format_number')
    def format_number(value):
        """Format number with commas."""
        try:
            return "{:,}".format(int(value))
        except (ValueError, TypeError):
            return value

    @app.template_filter('format_currency')
    def format_currency(value):
        """Format value as currency."""
        try:
            return "${:,.2f}".format(float(value))
        except (ValueError, TypeError):
            return value

    @app.template_filter('format_percent')
    def format_percent(value):
        """Format value as percentage."""
        try:
            return "{:.1f}%".format(float(value))
        except (ValueError, TypeError):
            return value

    # Health check endpoint (fast, for Railway health checks)
    @app.route('/health')
    def health_check():
        return {"status": "healthy", "service": "YouTube Analytics Dashboard"}

    # Warmup endpoint (pre-warms BigQuery connection)
    @app.route('/warmup')
    def warmup():
        """Pre-warm the app by initializing connections."""
        try:
            # Test BigQuery connection
            app.bigquery.get_channel_overview() if hasattr(app, 'bigquery') else None
            return {"status": "warmed", "bigquery": "connected"}
        except Exception as e:
            return {"status": "partial", "error": str(e)}, 200

    logging.info(f"Flask app created with config: {config_name}")
    return app
