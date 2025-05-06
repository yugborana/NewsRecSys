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

# News sources and topics
NEWS_SOURCES = [
    'https://www.cnn.com',
    'https://www.bbc.com',
    'https://www.reuters.com',
    'https://www.nytimes.com',
    'https://www.wsj.com',
    'https://www.aljazeera.com',
    'https://www.theguardian.com',
    'https://news.yahoo.com',
    'https://apnews.com',
    'https://www.bloomberg.com'
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