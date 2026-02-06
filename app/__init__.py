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

    # Health check endpoint
    @app.route('/health')
    def health_check():
        return {"status": "healthy", "service": "YouTube Analytics Dashboard"}

    logging.info(f"Flask app created with config: {config_name}")
    return app
