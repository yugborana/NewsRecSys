import asyncio
import aiohttp
import json
import logging
import time
from newspaper import Article
from kafka import KafkaConsumer, KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_NEWS_TOPIC, KAFKA_PROCESSED_TOPIC

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('article_scraper')

class AsyncArticleScraper:
    def __init__(self):
        """Initialize the article scraper with Kafka consumer and producer"""
        self.consumer = KafkaConsumer(
            KAFKA_NEWS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='article_scraper'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
    
    async def scrape_article(self, url, source, category=None):
        """Scrape a single article using newspaper3k"""
        try:
            # Create article object
            article = Article(url)
            
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
                
            return processed_article
            
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
        
        # Execute all scraping tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = 0
        for result in results:
            if result and not isinstance(result, Exception):
                # Send processed article to Kafka
                self.producer.send(KAFKA_PROCESSED_TOPIC, value=result)
                success_count += 1
                
        logger.info(f"Processed batch: {success_count} successful out of {len(tasks)}")
    
    async def run_scraper(self):
        """Run the scraper continuously"""
        batch_size = 10
        current_batch = []
        
        logger.info("Starting article scraper...")
        
        while True:
            try:
                # Poll for new messages with timeout
                message_pack = self.consumer.poll(timeout_ms=1000, max_records=batch_size)
                
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        news_item = message.value
                        current_batch.append(news_item)
                
                if current_batch:
                    logger.info(f"Processing batch of {len(current_batch)} articles")
                    await self.process_batch(current_batch)
                    current_batch = []
                else:
                    # If no messages, sleep briefly to avoid CPU spinning
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error in scraper loop: {e}")
                await asyncio.sleep(5)  # Brief sleep before retry
    
    def start(self):
        """Start the scraper event loop"""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run_scraper())