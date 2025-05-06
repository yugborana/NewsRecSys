import asyncio
import aiohttp
import json
import logging
import time
import os
from newspaper import Article, ArticleException
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_NEWS_TOPIC, KAFKA_PROCESSED_TOPIC,
    NEWSPAPER_CACHE_DIR
)

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('article_scraper')

class AsyncArticleScraper:
    def __init__(self, max_retries=5, retry_delay=10):
        """Initialize the article scraper with Kafka consumer and producer"""
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.setup_kafka()
    
    def setup_kafka(self):
        """Set up Kafka consumer and producer with retries"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, attempt {attempt+1}")
                
                # Setup consumer
                self.consumer = KafkaConsumer(
                    KAFKA_NEWS_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    group_id='article_scraper',
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                
                # Setup producer
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                
                # Test connections
                topics = self.consumer.topics()
                logger.info(f"Connected to Kafka. Available topics: {topics}")
                
                if KAFKA_NEWS_TOPIC not in topics:
                    logger.warning(f"Topic {KAFKA_NEWS_TOPIC} does not exist yet. It will be created when messages arrive.")
                
                return
            except NoBrokersAvailable:
                logger.warning(f"No Kafka brokers available. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Error connecting to Kafka: {e}")
                time.sleep(self.retry_delay)
        
        raise ConnectionError(f"Failed to connect to Kafka after {self.max_retries} attempts")
    
    async def scrape_article(self, url, source, category=None):
        """Scrape a single article using newspaper3k"""
        try:
            logger.info(f"Scraping article: {url}")
            
            # Create article object
            article = Article(url)
            
            # Configure newspaper
            article.config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            article.config.request_timeout = 15
            article.config.fetch_images = False
            article.config.cache_directory = NEWSPAPER_CACHE_DIR
            
            # Download and parse
            article.download()
            article.parse()
            
            # Run NLP to extract additional data
            article.nlp()
            
            # Create processed article data
            processed_article = {
                'title': article.title,
                'text': article.text,
                'summary': article.summary,
                'keywords': article.keywords,
                'url': url,
                'source': source,
                'authors': article.authors,
                'publish_date': str(article.publish_date) if article.publish_date else None,
                'top_image': article.top_image,
                'processed_at': time.time()
            }
            
            # Add category if available
            if category:
                processed_article['category'] = category
            
            logger.info(f"Successfully scraped article: {url}")
            return processed_article
            
        except ArticleException as e:
            logger.warning(f"Article exception for {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None
    
    async def process_batch(self, news_items):
        """Process a batch of news items asynchronously"""
        tasks = []
        for item in news_items:
            if 'url' in item:
                category = item.get('category', None)
                task = self.scrape_article(item['url'], item['source'], category)
                tasks.append(task)
        
        if not tasks:
            logger.warning("No valid items in batch to process")
            return
            
        # Execute all scraping tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = 0
        for result in results:
            if result and not isinstance(result, Exception):
                try:
                    # Send processed article to Kafka
                    future = self.producer.send(KAFKA_PROCESSED_TOPIC, value=result)
                    result_metadata = future.get(timeout=10)
                    logger.debug(f"Sent to {result_metadata.topic} partition {result_metadata.partition} offset {result_metadata.offset}")
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to send to Kafka: {e}")
        
        # Ensure messages are sent
        self.producer.flush()        
        logger.info(f"Processed batch: {success_count} successful out of {len(tasks)}")
    
    async def run_scraper(self):
        """Run the scraper continuously"""
        batch_size = 5  # Smaller batch size for more frequent processing
        current_batch = []
        
        logger.info("Starting article scraper...")
        
        while True:
            try:
                # Poll for new messages with timeout
                message_pack = self.consumer.poll(timeout_ms=1000, max_records=batch_size)
                
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        news_item = message.value
                        logger.debug(f"Received message: {news_item}")
                        current_batch.append(news_item)
                
                if current_batch:
                    logger.info(f"Processing batch of {len(current_batch)} articles")
                    await self.process_batch(current_batch)
                    current_batch = []
                else:
                    # If no messages, sleep briefly to avoid CPU spinning
                    await asyncio.sleep(1)
                    
            except KeyboardInterrupt:
                logger.info("Scraper interrupted. Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in scraper loop: {str(e)}")
                await asyncio.sleep(5)  # Brief sleep before retry
    
    def start(self):
        """Start the scraper event loop"""
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.run_scraper())
        except KeyboardInterrupt:
            logger.info("Scraper stopped")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    # For testing the scraper standalone
    scraper = AsyncArticleScraper()
    scraper.start()