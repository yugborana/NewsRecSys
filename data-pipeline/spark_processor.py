import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_PROCESSED_TOPIC

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('spark_processor')

# Define schema for processed articles
article_schema = StructType([
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("url", StringType(), True),
    StructField("source", StringType(), True),
    StructField("authors", ArrayType(StringType()), True),
    StructField("publish_date", StringType(), True),
    StructField("top_image", StringType(), True),
    StructField("category", StringType(), True),
    StructField("processed_at", TimestampType(), True)
])

def initialize_spark():
    """Initialize Spark session with Kafka packages"""
    return SparkSession.builder \
        .appName("NewsProcessingPipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

def save_to_redis_from_batch(batch_df, batch_id):
    """Save batch to Redis - this function will be passed to foreachBatch"""
    # We'll import the vector store module here to avoid circular imports
    from vector_store import VectorStore
    
    vector_store = VectorStore()
    
    for row in batch_df.collect():
        try:
            article_id = hash(row["url"])  # Generate ID from URL
            row_dict = row.asDict()
            
            # Prepare text for embedding (title + text)
            text_for_embedding = f"{row_dict.get('title', '')} {row_dict.get('text', '')}"
            
            # Generate embedding and store in Redis
            vector_store.store_article(article_id, row_dict, text_for_embedding)
            logger.info(f"Stored article {article_id} in Redis")
            
        except Exception as e:
            logger.error(f"Error saving article to Redis: {e}")

def process_with_spark():
    """Process news articles with Spark Streaming"""
    try:
        logger.info("Initializing Spark session")
        spark = initialize_spark()
        spark.sparkContext.setLogLevel("WARN")  # Reduce Spark logging verbosity
        
        logger.info("Setting up Kafka stream")
        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_PROCESSED_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), article_schema).alias("article")
        ).select("article.*")
        
        # Filter out articles with no text or title
        processed_df = parsed_df.filter(
            (col("text").isNotNull() & (col("text") != "")) & 
            (col("title").isNotNull() & (col("title") != ""))
        )
        
        # Write to console for debugging (optional)
        console_query = processed_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()
        
        # Write to Redis via the foreachBatch function
        redis_query = processed_df \
            .writeStream \
            .foreachBatch(save_to_redis_from_batch) \
            .outputMode("append") \
            .start()
        
        logger.info("Spark Streaming started, waiting for termination...")
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        logger.error(f"Error in Spark Streaming: {e}")
        raise