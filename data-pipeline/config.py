import os

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_NEWS_TOPIC = 'raw_news_articles'
KAFKA_PROCESSED_TOPIC = 'processed_news_articles'

# Redis settings
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
REDIS_INDEX_NAME = 'news_articles_idx'

# Embedding model settings
EMBEDDING_MODEL = 'sentence-transformers/msmarco-distilbert-base-v4'
VECTOR_SIZE = 768

# Newspaper cache
NEWSPAPER_CACHE_DIR = os.environ.get('NEWSPAPER_CACHE_DIR', '/tmp/newspaper-cache')
# Ensure cache directory exists
if not os.path.exists(NEWSPAPER_CACHE_DIR):
    try:
        os.makedirs(NEWSPAPER_CACHE_DIR, exist_ok=True)
    except:
        pass

# News sources and topics - selected sources that work better with newspaper3k
NEWS_SOURCES = [
    'https://www.reuters.com',
    'https://apnews.com',
    'https://www.theguardian.com/international',
    'https://www.npr.org',
    'https://www.bbc.com/news'
]

DEFAULT_TOPICS = [
    'technology',
    'business',
    'health',
    'science',
    'politics',
    'sports',
    'entertainment'
]

# Limit articles for development
DEV_MODE = os.getenv('DEV_MODE', 'True').lower() in ('true', '1', 't')
MAX_ARTICLES_PER_SOURCE = 5 if DEV_MODE else 20
MAX_ARTICLES_PER_CATEGORY = 3 if DEV_MODE else 10