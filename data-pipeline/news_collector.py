import time
import json
import logging
import newspaper
from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_NEWS_TOPIC, NEWS_SOURCES, DEFAULT_TOPICS

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('news_collector')

class NewsCollector:
    def __init__(self):
        """Initialize the news collector with Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.sources = NEWS_SOURCES
        
    def collect_from_source(self, source_url):
        """Collect news articles from a single source"""
        try:
            logger.info(f"Building news source from {source_url}")
            news_source = newspaper.build(source_url, memoize_articles=False)
            
            logger.info(f"Found {len(news_source.articles)} articles from {source_url}")
            article_count = 0
            
            for article in news_source.articles[:20]:  # Limit to 20 articles per source for testing
                try:
                    # Just get the URL and basic info, full processing comes later
                    news_item = {
                        'url': article.url,
                        'source': source_url,
                        'collected_at': time.time()
                    }
                    
                    # Send to Kafka
                    self.producer.send(KAFKA_NEWS_TOPIC, value=news_item)
                    article_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing article {article.url}: {e}")
            
            logger.info(f"Sent {article_count} articles from {source_url} to Kafka")
            
        except Exception as e:
            logger.error(f"Error collecting from {source_url}: {e}")
    
    def collect_by_category(self, source_url, category):
        """Collect news by specific category"""
        try:
            logger.info(f"Building category {category} from {source_url}")
            news_source = newspaper.build(f"{source_url}/{category}", memoize_articles=False)
            
            logger.info(f"Found {len(news_source.articles)} articles in category {category}")
            article_count = 0
            
            for article in news_source.articles[:10]:  # Limit articles per category
                try:
                    news_item = {
                        'url': article.url,
                        'source': source_url,
                        'category': category,
                        'collected_at': time.time()
                    }
                    
                    # Send to Kafka
                    self.producer.send(KAFKA_NEWS_TOPIC, value=news_item)
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
                # Collect from main pages
                for source in self.sources:
                    self.collect_from_source(source)
                    time.sleep(5)  # Brief pause between sources
                
                # Collect from category pages for sources that support it
                for source in self.sources:
                    for category in categories:
                        self.collect_by_category(source, category)
                        time.sleep(5)  # Brief pause between categories
                
                logger.info(f"Completed collection cycle. Sleeping for {interval} seconds...")
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in collector main loop: {e}")
                logger.info("Sleeping for 5 minutes before retry...")
                time.sleep(300)