import os
import argparse
import multiprocessing
import logging
from news_collector import NewsCollector
from article_scraper import AsyncArticleScraper
import spark_processor
import uvicorn
from config import DEFAULT_TOPICS

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('main')

def run_news_collector(topics):
    """Run the news collector with specified topics"""
    logger.info(f"Starting news collector with topics: {topics}")
    collector = NewsCollector()
    collector.run_collector(categories=topics)

def run_article_scraper():
    """Run the article scraper"""
    logger.info("Starting article scraper")
    scraper = AsyncArticleScraper()
    scraper.start()

def run_spark_processor():
    """Run the Spark processor"""
    logger.info("Starting Spark processor")
    spark_processor.process_with_spark()

def run_api():
    """Run the API server"""
    logger.info("Starting API server")
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="News Processing Pipeline")
    parser.add_argument("--topics", nargs="+", default=DEFAULT_TOPICS, 
                        help="List of topics to collect news for")
    parser.add_argument("--mode", choices=["all", "collector", "scraper", "processor", "api"], 
                        default="all", help="Component to run")
    
    args = parser.parse_args()
    
    if args.mode == "collector":
        run_news_collector(args.topics)
    elif args.mode == "scraper":
        run_article_scraper()
    elif args.mode == "processor":
        run_spark_processor()
    elif args.mode == "api":
        run_api()
    else:
        # Create and start all processes
        processes = []
        
        collector_process = multiprocessing.Process(
            target=run_news_collector,
            args=(args.topics,)
        )
        processes.append(collector_process)
        
        scraper_process = multiprocessing.Process(
            target=run_article_scraper
        )
        processes.append(scraper_process)
        
        spark_process = multiprocessing.Process(
            target=run_spark_processor
        )
        processes.append(spark_process)
        
        api_process = multiprocessing.Process(
            target=run_api
        )
        processes.append(api_process)
        
        # Start all processes
        for process in processes:
            process.start()
        
        # Wait for all processes to complete
        for process in processes:
            process.join()