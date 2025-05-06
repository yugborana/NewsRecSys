import time
import json
import logging
import newspaper
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_NEWS_TOPIC, NEWS_SOURCES, DEFAULT_TOPICS,
    NEWSPAPER_CACHE_DIR, MAX_ARTICLES_PER_SOURCE, MAX_ARTICLES_PER_CATEGORY
)

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('news_collector')

class NewsCollector:
    def __init__(self, retry_limit=5, retry_delay=10):
        """Initialize the news collector with Kafka producer"""
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.producer = None
        self.connect_kafka()
        self.sources = NEWS_SOURCES
        
    def connect_kafka(self):
        """Connect to Kafka with retries"""
        attempt = 0
        while attempt < self.retry_limit:
            try:
                logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, attempt {attempt+1}")
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info("Successfully connected to Kafka")
                # Test connection by sending a test message
                self.producer.send('__test', value={"test": "connection"})
                self.producer.flush()
                return
            except NoBrokersAvailable:
                logger.warning(f"No Kafka brokers available. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
                attempt += 1
            except Exception as e:
                logger.error(f"Failed to connect to Kafka: {e}")
                time.sleep(self.retry_delay)
                attempt += 1
                
        logger.error(f"Failed to connect to Kafka after {self.retry_limit} attempts")
        raise ConnectionError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        
    def collect_from_source(self, source_url):
        """Collect news articles from a single source"""
        try:
            logger.info(f"Building news source from {source_url}")
            
            # Configure newspaper with cache directory
            config = newspaper.Config()
            config.memoize_articles = False
            config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            config.request_timeout = 10
            config.fetch_images = False
            config.cache_directory = NEWSPAPER_CACHE_DIR
            
            # Build the source with our configuration
            news_source = newspaper.build(source_url, config=config)
            
            logger.info(f"Found {len(news_source.articles)} articles from {source_url}")
            article_count = 0
            
            for article in news_source.articles[:MAX_ARTICLES_PER_SOURCE]:
                try:
                    # Just get the URL and basic info, full processing comes later
                    news_item = {
                        'url': article.url,
                        'source': source_url,
                        'collected_at': time.time()
                    }
                    
                    # Send to Kafka
                    future = self.producer.send(KAFKA_NEWS_TOPIC, value=news_item)
                    
                    # Optional: Wait for the message to be delivered
                    record_metadata = future.get(timeout=10)
                    logger.debug(f"Message sent to {record_metadata.topic} [{record_metadata.partition}] @ {record_metadata.offset}")
                    
                    article_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing article {article.url}: {e}")
            
            logger.info(f"Sent {article_count} articles from {source_url} to Kafka")
            
        except Exception as e:
            logger.error(f"Error collecting from {source_url}: {e}")
    
    def collect_by_category(self, source_url, category):
        """Collect news by specific category"""
        try:
            category_url = f"{source_url}/{category}"
            logger.info(f"Building category {category} from {category_url}")
            
            # Configure newspaper with cache directory
            config = newspaper.Config()
            config.memoize_articles = False
            config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            config.request_timeout = 10
            config.fetch_images = False
            config.cache_directory = NEWSPAPER_CACHE_DIR
            
            # Build the source with our configuration
            news_source = newspaper.build(category_url, config=config)
            
            logger.info(f"Found {len(news_source.articles)} articles in category {category}")
            article_count = 0
            
            for article in news_source.articles[:MAX_ARTICLES_PER_CATEGORY]:
                try:
                    news_item = {
                        'url': article.url,
                        'source': source_url,
                        'category': category,
                        'collected_at': time.time()
                    }
                    
                    # Send to Kafka
                    future = self.producer.send(KAFKA_NEWS_TOPIC, value=news_item)
                    
                    # Optional: Wait for the message to be delivered
                    record_metadata = future.get(timeout=10)
                    logger.debug(f"Message sent to {record_metadata.topic} [{record_metadata.partition}] @ {record_metadata.offset}")
                    
                    article_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing article in category {category}: {e}")
            
            logger.info(f"Sent {article_count} articles from category {category} to Kafka")
            
        except Exception as e:
            logger.error(f"Error collecting category {category} from {source_url}: {e}")
    
    def run_collector(self, interval=3600, categories=None):
        """Run the collector continuously"""
        if categories is None:
            categories = DEFAULT_TOPICS
            
        while True:
            try:
                logger.info("Starting collection cycle")
                
                # Ensure Kafka connection is alive
                if self.producer is None:
                    self.connect_kafka()
                
                # Collect from main pages
                for source in self.sources:
                    try:
                        self.collect_from_source(source)
                        time.sleep(5)  # Brief pause between sources
                    except Exception as e:
                        logger.error(f"Error processing source {source}: {e}")
                
                # Collect from category pages for sources that support it
                for source in self.sources:
                    for category in categories:
                        try:
                            self.collect_by_category(source, category)
                            time.sleep(5)  # Brief pause between categories
                        except Exception as e:
                            logger.error(f"Error processing category {category} for {source}: {e}")
                
                # Make sure all messages are sent
                self.producer.flush()
                
                logger.info(f"Completed collection cycle. Sleeping for {interval} seconds...")
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("Collection interrupted. Shutting down...")
                if self.producer:
                    self.producer.close()
                break
            except Exception as e:
                logger.error(f"Error in collector main loop: {e}")
                logger.info("Sleeping for 5 minutes before retry...")
                time.sleep(300)

if __name__ == "__main__":
    # For testing the collector standalone
    collector = NewsCollector()
    collector.run_collector(interval=900)  # 15 minutes interval for testing