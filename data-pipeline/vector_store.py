import json
import logging
import numpy as np
import redis
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
    def __init__(self):
        """Initialize the vector store with Redis and embedding model"""
        # Connect to Redis
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=False  # Keep binary for vector data
        )
        
        # Initialize the embedding model
        logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
        self.model = SentenceTransformer(EMBEDDING_MODEL)
        
        # Create Redis Search index if it doesn't exist
        self._create_index()
    
    def _create_index(self):
        """Create Redis Search index for vector search"""
        try:
            # Check if index exists
            try:
                self.redis_client.execute_command("FT.INFO", REDIS_INDEX_NAME)
                logger.info(f"Index {REDIS_INDEX_NAME} already exists")
                return
            except redis.exceptions.ResponseError:
                # Index doesn't exist, continue to creation
                pass
            
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
                "title": article_data.get("title", ""),
                "text": article_data.get("text", ""),
                "summary": article_data.get("summary", ""),
                "url": article_data.get("url", ""),
                "source": article_data.get("source", ""),
                "category": article_data.get("category", "general"),
                "authors": json.dumps(article_data.get("authors", [])),
                "publish_date": article_data.get("publish_date", ""),
                "embedding": embedding_bytes
            }
            
            # Store in Redis
            self.redis_client.hset(f"article:{article_id}", mapping=redis_data)
            return True
            
        except Exception as e:
            logger.error(f"Error storing article {article_id} in Redis: {e}")
            return False
    
    def search_similar(self, query_text, limit=5, filters=None):
        """Search for similar articles using vector similarity"""
        try:
            # Generate embedding for query
            query_embedding = self.generate_embedding(query_text)
            
            # Convert to bytes for Redis query
            query_embedding_bytes = np.array(query_embedding, dtype=np.float32).tobytes()
            
            # Build query
            base_query = f"*=>[KNN {limit} @embedding $query_vector AS score]"
            
            # Add filters if provided
            if filters:
                filter_parts = []
                for key, value in filters.items():
                    filter_parts.append(f"@{key}:{{{value}}}")
                
                if filter_parts:
                    filter_str = " ".join(filter_parts)
                    base_query = f"{filter_str} {base_query}"
            
            # Execute vector search
            results = self.redis_client.execute_command(
                "FT.SEARCH", REDIS_INDEX_NAME,
                base_query,
                "PARAMS", "2", "query_vector", query_embedding_bytes,
                "RETURN", "6", "title", "summary", "url", "source", "category", "score",
                "SORTBY", "score", "ASC"
            )
            
            # Parse results
            parsed_results = []
            if results and len(results) > 1:  # First element is count
                # Skip the first element (count) and process pairs
                for i in range(1, len(results), 2):
                    article_id = results[i].decode('utf-8')
                    properties = results[i+1]
                    
                    # Convert to dict for easier handling
                    result_dict = {}
                    for j in range(0, len(properties), 2):
                        key = properties[j].decode('utf-8')
                        value = properties[j+1].decode('utf-8') if properties[j+1] else ""
                        result_dict[key] = value
                    
                    # Add article_id (without the 'article:' prefix)
                    result_dict['id'] = article_id.replace('article:', '')
                    
                    parsed_results.append(result_dict)
            
            return parsed_results
            
        except Exception as e:
            logger.error(f"Error searching in Redis: {e}")
            return []