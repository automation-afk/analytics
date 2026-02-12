"""Flask application configuration."""
import os
from datetime import timedelta
from dotenv import load_dotenv

# Load environment variables from .env.web
load_dotenv('.env.web')


class Config:
    """Base configuration."""

    # Flask settings
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')

    # Session settings - prevent stale sessions
    SESSION_COOKIE_SECURE = True  # Only send over HTTPS in production
    SESSION_COOKIE_HTTPONLY = True  # Prevent JS access
    SESSION_COOKIE_SAMESITE = 'Lax'  # CSRF protection
    PERMANENT_SESSION_LIFETIME = timedelta(hours=8)  # Session expires after 8 hours

    # BigQuery settings
    BIGQUERY_PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID', 'company-wide-370010')
    GOOGLE_CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH')

    # AI APIs
    ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

    # Cache settings
    CACHE_TYPE = os.getenv('CACHE_TYPE', 'simple')
    CACHE_DEFAULT_TIMEOUT = int(os.getenv('CACHE_DEFAULT_TIMEOUT', 300))

    # Pagination
    VIDEOS_PER_PAGE = int(os.getenv('VIDEOS_PER_PAGE', 25))

    # Analysis settings
    MAX_CONCURRENT_ANALYSES = int(os.getenv('MAX_CONCURRENT_ANALYSES', 5))
    ANALYSIS_RATE_LIMIT_SECONDS = int(os.getenv('ANALYSIS_RATE_LIMIT_SECONDS', 2))

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # YouTube Channel
    YOUTUBE_CHANNEL_ID = os.getenv('YOUTUBE_CHANNEL_ID', '@homesecurityheroes')

    # Preferred brands per silo (used for CTA scoring penalty)
    # If a video's description/CTA doesn't mention the preferred brand for its silo,
    # the CTA score gets dinged by 50%
    PREFERRED_BRANDS = {
        'identitytheft': 'Aura',
        'database': 'Aura',
        'PC': 'Aura',
    }


class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    TESTING = False
    SESSION_COOKIE_SECURE = False  # Allow HTTP in development


class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    TESTING = False
    # Use simple cache by default, redis if REDIS_URL is set
    CACHE_TYPE = os.getenv('CACHE_TYPE', 'simple')
    CACHE_REDIS_URL = os.getenv('REDIS_URL')


class TestingConfig(Config):
    """Testing configuration."""
    DEBUG = True
    TESTING = True
    CACHE_TYPE = 'simple'


# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}
