import json
import logging
import numpy as np
import time
import redis
import os
from sentence_transformers import SentenceTransformer
from config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    REDIS_INDEX_NAME, EMBEDDING_MODEL, VECTOR_SIZE
)

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('vector_store')

class VectorStore:
    def __init__(self, max_retries=5, retry_delay=10):
        """Initialize the vector store with Redis and embedding model"""
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.redis_client = None
        self.model = None
        
        # Connect to Redis with retries
        self.connect_redis()
        
        # Initialize the embedding model
        self.initialize_model()
        
        # Create Redis Search index if it doesn't exist
        self._create_index()
    
    def connect_redis(self):
        """Connect to Redis with retries"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}, attempt {attempt+1}")
                self.redis_client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    password=REDIS_PASSWORD,
                    decode_responses=False,  # Keep binary for vector data
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                
                # Test connection
                self.redis_client.ping()
                logger.info("Successfully connected to Redis")
                return
            except redis.exceptions.ConnectionError:
                logger.warning(f"Could not connect to Redis. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Error connecting to Redis: {e}")
                time.sleep(self.retry_delay)
        
        raise ConnectionError(f"Failed to connect to Redis after {self.max_retries} attempts")
    
    def initialize_model(self):
        """Initialize the embedding model with error handling"""
        try:
            logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
            self.model = SentenceTransformer(EMBEDDING_MODEL)
            logger.info("Model loaded successfully")
        except Exception as e:
            logger.error(f"Error loading embedding model: {e}")
            raise
    
    def _create_index(self):
        """Create Redis Search index for vector search"""
        try:
            # Check if index exists
            try:
                self.redis_client.execute_command("FT.INFO", REDIS_INDEX_NAME)
                logger.info(f"Index {REDIS_INDEX_NAME} already exists")
                return
            except redis.exceptions.ResponseError as e:
                if "unknown index name" not in str(e).lower():
                    logger.error(f"Unexpected Redis error: {e}")
                    raise
                # Index doesn't exist, continue to creation
                logger.info(f"Index {REDIS_INDEX_NAME} doesn't exist, creating it...")
            
            # Create the index with vector search capabilities
            self.redis_client.execute_command(
                "FT.CREATE", REDIS_INDEX_NAME,
                "ON", "HASH",
                "PREFIX", "1", "article:",
                "SCHEMA",
                "title", "TEXT", "WEIGHT", "5.0",
                "text", "TEXT",
                "summary", "TEXT", "WEIGHT", "3.0",
                "source", "TAG",
                "category", "TAG",
                "url", "TEXT",
                "embedding", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", str(VECTOR_SIZE), "DISTANCE_METRIC", "COSINE"
            )
            logger.info(f"Created index {REDIS_INDEX_NAME}")
            
        except Exception as e:
            logger.error(f"Error creating Redis index: {e}")
            raise
    
    def generate_embedding(self, text):
        """Generate embedding for a text using the sentence transformer model"""
        if not text or len(text.strip()) == 0:
            raise ValueError("Cannot generate embedding for empty text")
        
        # Truncate extremely long texts to avoid memory issues
        max_length = 10000  # Maximum number of characters
        if len(text) > max_length:
            logger.warning(f"Text truncated from {len(text)} to {max_length} characters")
            text = text[:max_length]
        
        embedding = self.model.encode(text)
        return embedding
    
    def store_article(self, article_id, article_data, text_for_embedding):
        """Store article and its embedding in Redis"""
        try:
            # Generate embedding
            embedding = self.generate_embedding(text_for_embedding)
            
            # Convert embedding to bytes for Redis
            embedding_bytes = np.array(embedding, dtype=np.float32).tobytes()
            
            # Prepare data for Redis
            redis_data = {
                'title': article_data.get('title', ''),
                'text': article_data.get('text', ''),
                'summary': article_data.get('summary', ''),
                'source': article_data.get('source', ''),
                'url': article_data.get('url', ''),
                'category': article_data.get('category', ''),
                'keywords': json.dumps(article_data.get('keywords', [])),
                'authors': json.dumps(article_data.get('authors', [])),
                'publish_date': article_data.get('publish_date', ''),
                'embedding': embedding_bytes
            }
            
            # Store in Redis
            self.redis_client.hset(f"article:{article_id}", mapping=redis_data)
            logger.info(f"Stored article {article_id} in Redis with embedding")
            
            return True
        except Exception as e:
            logger.error(f"Error storing article {article_id}: {e}")
            return False
    
    def search_similar(self, query_text, limit=5, filters=None):
        """Search for similar articles using vector similarity"""
        try:
            # Generate embedding for query
            query_embedding = self.generate_embedding(query_text)
            query_embedding_bytes = np.array(query_embedding, dtype=np.float32).tobytes()
            
            # Build filter string if filters are provided
            filter_str = ""
            if filters:
                filter_parts = []
                for key, value in filters.items():
                    filter_parts.append(f"@{key}:{{{value}}}")
                if filter_parts:
                    filter_str = " ".join(filter_parts)
            
            # Execute vector search
            query = f"*=>[KNN {limit} @embedding $embedding]"
            if filter_str:
                query = f"{filter_str} {query}"
            
            results = self.redis_client.execute_command(
                "FT.SEARCH", REDIS_INDEX_NAME,
                query,
                "PARAMS", "2", "embedding", query_embedding_bytes,
                "SORTBY", "_vector_score",
                "LIMIT", "0", str(limit),
                "RETURN", "7", "title", "summary", "url", "source", "category", "_vector_score", "id"
            )
            
            # Parse results
            formatted_results = []
            if results and len(results) > 1:  # First item is count
                for i in range(1, len(results), 2):  # Skip count and iterate through pairs
                    article_id = results[i].decode('utf-8')
                    article_data = {}
                    
                    # Convert result data to dict
                    data = results[i+1]
                    for j in range(0, len(data), 2):
                        key = data[j].decode('utf-8')
                        value = data[j+1].decode('utf-8') if data[j+1] else ""
                        article_data[key] = value
                    
                    # Add score
                    score = float(article_data.get('_vector_score', 0.0))
                    article_data['score'] = 1.0 - score  # Convert to similarity (1.0 is perfect match)
                    article_data['id'] = article_id.replace('article:', '')
                    
                    formatted_results.append(article_data)
            
            return formatted_results
        except Exception as e:
            logger.error(f"Error searching similar articles: {e}")
            return []