"""
Configuration Management for Uganda eHMIS Analytics
Supports 10,000+ concurrent users with proper scaling
"""
import os
from datetime import timedelta

class Config:
    """Base configuration"""
    # Security
    SECRET_KEY = os.getenv('SECRET_KEY', 'uganda-ehmis-production-key-2024-secure')
    
    # Session Configuration (for 10,000 users)
    SESSION_TYPE = 'redis' if os.getenv('REDIS_URL') else 'filesystem'
    SESSION_PERMANENT = True
    PERMANENT_SESSION_LIFETIME = timedelta(hours=8)
    SESSION_USE_SIGNER = True
    SESSION_KEY_PREFIX = 'ehmis:'
    
    # Redis for sessions and caching (production)
    REDIS_URL = os.getenv('REDIS_URL', None)
    
    # DHIS2 Configuration
    DHIS2_BASE_URL = os.getenv('DHIS2_BASE_URL', 'https://hmis.health.go.ug/api')
    DHIS2_TIMEOUT = int(os.getenv('DHIS2_TIMEOUT', '60'))
    
    # Rate Limiting (per user)
    RATELIMIT_DEFAULT = "200 per minute"
    RATELIMIT_STORAGE_URL = os.getenv('REDIS_URL', 'memory://')
    
    # Caching TTLs (seconds)
    CACHE_ORG_UNITS_TTL = 3600      # 1 hour - org units rarely change
    CACHE_DATA_ELEMENTS_TTL = 3600  # 1 hour - data elements rarely change
    CACHE_ANALYTICS_TTL = 300       # 5 minutes - analytics data changes
    CACHE_SEARCH_TTL = 600          # 10 minutes - search results
    
    # Connection Pooling
    REQUESTS_POOL_SIZE = 100
    REQUESTS_MAX_RETRIES = 3
    
    # CORS
    CORS_ORIGINS = os.getenv('CORS_ORIGINS', '*')


class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    SESSION_TYPE = 'filesystem'
    

class ProductionConfig(Config):
    """Production configuration for 10,000+ users"""
    DEBUG = False
    
    # Force Redis for production
    SESSION_TYPE = 'redis'
    
    # Stricter rate limiting
    RATELIMIT_DEFAULT = "100 per minute"
    
    # Longer cache TTLs for production
    CACHE_ORG_UNITS_TTL = 7200      # 2 hours
    CACHE_DATA_ELEMENTS_TTL = 7200  # 2 hours


class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    DEBUG = True


# Configuration selector
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}

def get_config():
    """Get configuration based on environment"""
    env = os.getenv('FLASK_ENV', 'development')
    return config.get(env, config['default'])




