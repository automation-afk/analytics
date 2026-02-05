"""Flask application configuration."""
import os
from dotenv import load_dotenv

# Load environment variables from .env.web
load_dotenv('.env.web')


class Config:
    """Base configuration."""

    # Flask settings
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')

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


class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    TESTING = False


class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    TESTING = False
    CACHE_TYPE = 'redis'  # Use Redis in production
    CACHE_REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')


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
