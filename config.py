"""
Configuration management for Meetup GNN application - Production Ready
"""
import os
from pathlib import Path
from datetime import timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Base configuration"""
    # App settings
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-jwt-secret-change-in-production")

    # Database
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///instance/meetup.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_size': int(os.getenv("DB_POOL_SIZE", "10")),
        'pool_recycle': int(os.getenv("DB_POOL_RECYCLE", "3600")),
        'pool_pre_ping': True,
        'max_overflow': int(os.getenv("DB_MAX_OVERFLOW", "20")),
    }

    # Redis Cache (NUOVO!)
    CACHE_TYPE = os.getenv("CACHE_TYPE", "SimpleCache")  # SimpleCache for dev, redis for prod
    CACHE_REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    CACHE_REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    CACHE_REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    CACHE_REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
    CACHE_REDIS_URL = os.getenv("REDIS_URL") or f'redis://{CACHE_REDIS_HOST}:{CACHE_REDIS_PORT}/{CACHE_REDIS_DB}'
    CACHE_DEFAULT_TIMEOUT = int(os.getenv("CACHE_TIMEOUT", "300"))  # 5 minutes
    CACHE_KEY_PREFIX = "meetup:"

    # Session Management (NUOVO!)
    SESSION_TYPE = 'redis' if CACHE_TYPE == 'redis' else 'filesystem'
    SESSION_PERMANENT = False
    SESSION_USE_SIGNER = True
    SESSION_KEY_PREFIX = "session:"

    # API Rate Limiting (NUOVO!)
    RATELIMIT_ENABLED = os.getenv("RATELIMIT_ENABLED", "true").lower() == "true"
    RATELIMIT_STORAGE_URL = CACHE_REDIS_URL
    RATELIMIT_STRATEGY = "fixed-window"
    RATELIMIT_DEFAULT = os.getenv("RATELIMIT_DEFAULT", "100/hour")
    RATELIMIT_HEADERS_ENABLED = True

    # Admin settings
    ADMIN_EMAILS = os.getenv("ADMIN_EMAILS", "martin.stamenov03@gmail.com").split(',')

    # GNN settings
    GNN_GRAPH_PATH = os.getenv("GNN_GRAPH_PATH", "graph_construction/graph_data/event_similarity_graph.pt")
    GNN_ALPHA = float(os.getenv("GNN_ALPHA", "0.6"))
    GNN_CACHE_EMBEDDINGS = os.getenv("GNN_CACHE_EMBEDDINGS", "true").lower() == "true"
    GNN_BATCH_SIZE = int(os.getenv("GNN_BATCH_SIZE", "100"))

    # Scraper settings
    SCRAPER_TIMEOUT = int(os.getenv("SCRAPER_TIMEOUT", "1800"))
    SCRAPER_HEADLESS = os.getenv("SCRAPER_HEADLESS", "true").lower() == "true"

    # Logging (УЛУЧШЕНО!)
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "logs/app.log")
    LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "10485760"))  # 10MB
    LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # JWT
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(seconds=int(os.getenv("JWT_ACCESS_TOKEN_EXPIRES", "604800")))  # 7 days
    JWT_REFRESH_TOKEN_EXPIRES = timedelta(days=int(os.getenv("JWT_REFRESH_TOKEN_EXPIRES", "30")))

    # Performance (NUOVO!)
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max upload
    PROPAGATE_EXCEPTIONS = True

    # Monitoring (NUOVO!)
    SENTRY_DSN = os.getenv("SENTRY_DSN")
    SENTRY_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    SENTRY_TRACES_SAMPLE_RATE = float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1"))

    # Data directories
    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = BASE_DIR / "data_collection"
    CLEANED_DATA_DIR = DATA_DIR / "NLP_data" / "cleaned_data"
    RAW_DATA_DIR = DATA_DIR / "raw_data"
    PROCESSED_DATA_DIR = DATA_DIR / "processed_data"
    GRAPH_DATA_DIR = BASE_DIR / "graph_construction" / "graph_data"


class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    TESTING = False
    SQLALCHEMY_ECHO = True

    # Use simple cache for development if Redis not available
    CACHE_TYPE = 'SimpleCache'
    RATELIMIT_ENABLED = False  # Disable rate limiting in dev

    # More verbose logging
    LOG_LEVEL = "DEBUG"


class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    TESTING = False
    SQLALCHEMY_ECHO = False

    # Force Redis and PostgreSQL in production
    CACHE_TYPE = 'redis'
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/meetup")

    # Enable all performance features
    RATELIMIT_ENABLED = True
    GNN_CACHE_EMBEDDINGS = True

    # Override with stronger defaults for production
    SECRET_KEY = os.getenv("SECRET_KEY")  # Must be set in production
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")  # Must be set in production

    # Security
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'

    @classmethod
    def validate(cls):
        """Validate production configuration"""
        if not cls.SECRET_KEY or cls.SECRET_KEY == "dev-secret-key-change-in-production":
            raise ValueError("SECRET_KEY must be set in production!")
        if not cls.JWT_SECRET_KEY or cls.JWT_SECRET_KEY == "dev-jwt-secret-change-in-production":
            raise ValueError("JWT_SECRET_KEY must be set in production!")
        if 'sqlite' in cls.SQLALCHEMY_DATABASE_URI.lower():
            raise ValueError("SQLite is not recommended for production! Use PostgreSQL.")


class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///test.db"
    WTF_CSRF_ENABLED = False


# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config(env=None):
    """Get configuration based on environment"""
    if env is None:
        env = os.getenv('FLASK_ENV', 'development')

    return config.get(env, config['default'])
