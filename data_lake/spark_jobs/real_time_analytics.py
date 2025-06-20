#!/usr/bin/env python3
"""
Real-time Gaming Analytics dengan Apache Spark Streaming (Fixed untuk Kafka Integration)
Processes streaming data dari Kafka untuk real-time insights
"""

import os
import sys
import findspark

# Initialize findspark untuk auto-configure Spark
try:
    findspark.init()
except:
    pass

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealTimeGamingAnalytics:
    """
    Real-time analytics engine untuk gaming data dengan Spark Streaming (Fixed)
    """
    
    def __init__(self, 
                 kafka_bootstrap_servers="localhost:9092",
                 minio_endpoint="localhost:9000",
                 minio_access_key="minioadmin",
                 minio_secret_key="minioadmin"):
        """
        Initialize Spark session dengan Kafka packages dan configuration
        """
        self.kafka_servers = kafka_bootstrap_servers
        self.minio_endpoint = minio_endpoint
        
        # Kafka packages untuk Spark Streaming (Fixed versions)
        kafka_packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        ]
        
        # Initialize Spark session dengan packages yang diperlukan (simplified)
        builder = SparkSession.builder \
            .appName("RealTimeGamingAnalytics") \
            .config("spark.jars.packages", ",".join(kafka_packages)) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        
        # MinIO S3 configuration
        if minio_endpoint and minio_access_key and minio_secret_key:
            builder = builder \
                .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
                .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.hadoop.fs.s3a.attempts.maximum", "1") \
                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
                .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        
        try:
            self.spark = builder.getOrCreate()
            logger.info("✅ Spark session created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            # Fallback tanpa MinIO jika ada masalah
            self.spark = SparkSession.builder \
                .appName("RealTimeGamingAnalytics") \
                .config("spark.jars.packages", ",".join(kafka_packages)) \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .getOrCreate()
            logger.info("✅ Spark session created (fallback mode)")
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Define schemas untuk different event types
        self.schemas = self._define_schemas()
        
        logger.info("✅ Spark session initialized for real-time analytics")
    
    def _define_schemas(self):
        """Define schemas untuk different event types"""
        schemas = {}
        
        # Player events schema
        schemas['player_events'] = StructType([
            StructField("event_id", StringType(), True),
            StructField("player_id", StringType(), True),
            StructField("game_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),  # Changed to String untuk parsing
            StructField("server_region", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("metadata", StructType([
                StructField("level", IntegerType(), True),
                StructField("score", IntegerType(), True),
                StructField("playtime_session", IntegerType(), True),
                StructField("ip_hash", StringType(), True)
            ]), True)
        ])
        
        # Game reviews schema
        schemas['game_reviews'] = StructType([
            StructField("review_id", StringType(), True),
            StructField("player_id", StringType(), True),
            StructField("game_id", StringType(), True),
            StructField("rating", IntegerType(), True),
            StructField("review_text", StringType(), True),
            StructField("timestamp", StringType(), True),  # Changed to String
            StructField("playtime_hours", IntegerType(), True),
            StructField("verified_purchase", BooleanType(), True),
            StructField("helpful_votes", IntegerType(), True),
            StructField("total_votes", IntegerType(), True),
            StructField("sentiment_label", StringType(), True)
        ])
        
        # Server logs schema
        schemas['server_logs'] = StructType([
            StructField("log_id", StringType(), True),
            StructField("timestamp", StringType(), True),  # Changed to String
            StructField("level", StringType(), True),
            StructField("service", StringType(), True),
            StructField("server_id", StringType(), True),
            StructField("message", StringType(), True),
            StructField("player_id", StringType(), True),
            StructField("game_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("memory_usage_mb", IntegerType(), True),
            StructField("cpu_usage_percent", IntegerType(), True)
        ])
        
        return schemas
    
    def create_kafka_stream(self, topic: str):
        """Create streaming DataFrame dari Kafka topic dengan error handling"""
        try:
            logger.info(f"🔗 Creating Kafka stream for topic: {topic}")
            
            stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("maxOffsetsPerTrigger", 1000) \
                .option("kafka.consumer.timeout.ms", "30000") \
                .load()
            
            logger.info(f"✅ Created Kafka stream for topic: {topic}")
            return stream
            
        except Exception as e:
            logger.error(f"❌ Failed to create stream for {topic}: {e}")
            return None
    
    def process_player_events_stream(self):
        """Process real-time player events untuk analytics"""
        # Create stream
        raw_stream = self.create_kafka_stream("gaming.player.events")
        if not raw_stream:
            return []
        
        try:
            # Parse JSON data dengan error handling
            parsed_stream = raw_stream.select(
                from_json(col("value").cast("string"), self.schemas['player_events']).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp") \
            .filter(col("data").isNotNull()) \
            .withColumn("timestamp_parsed", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
            
            # Real-time aggregations dengan simplified windows
            # 1. Player activity per minute
            player_activity = parsed_stream \
                .withWatermark("timestamp_parsed", "2 minutes") \
                .groupBy(
                    window(col("timestamp_parsed"), "1 minute"),
                    col("server_region"),
                    col("platform")
                ) \
                .agg(
                    countDistinct("player_id").alias("unique_players"),
                    count("*").alias("total_events"),
                    avg("metadata.playtime_session").alias("avg_session_time")
                )
            
            # Write stream dengan error handling
            query1 = player_activity.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 20) \
                .trigger(processingTime="30 seconds") \
                .queryName("player_activity_console") \
                .start()
            
            logger.info("✅ Player events stream started")
            return [query1]
            
        except Exception as e:
            logger.error(f"❌ Error processing player events: {e}")
            return []
    
    def process_game_reviews_stream(self):
        """Process real-time game reviews untuk sentiment analysis"""
        raw_stream = self.create_kafka_stream("gaming.reviews.new")
        if not raw_stream:
            return []
        
        try:
            parsed_stream = raw_stream.select(
                from_json(col("value").cast("string"), self.schemas['game_reviews']).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp") \
            .filter(col("data").isNotNull()) \
            .withColumn("timestamp_parsed", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
            
            # Real-time sentiment aggregation
            sentiment_analysis = parsed_stream \
                .withWatermark("timestamp_parsed", "5 minutes") \
                .groupBy(
                    window(col("timestamp_parsed"), "5 minutes"),
                    col("game_id"),
                    col("sentiment_label")
                ) \
                .agg(
                    count("*").alias("review_count"),
                    avg("rating").alias("avg_rating")
                )
            
            query1 = sentiment_analysis.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 15) \
                .trigger(processingTime="1 minute") \
                .queryName("sentiment_analysis_console") \
                .start()
            
            logger.info("✅ Game reviews stream started")
            return [query1]
            
        except Exception as e:
            logger.error(f"❌ Error processing game reviews: {e}")
            return []
    
    def process_server_logs_stream(self):
        """Process real-time server logs untuk monitoring"""
        raw_stream = self.create_kafka_stream("gaming.server.logs")
        if not raw_stream:
            return []
        
        try:
            parsed_stream = raw_stream.select(
                from_json(col("value").cast("string"), self.schemas['server_logs']).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp") \
            .filter(col("data").isNotNull()) \
            .withColumn("timestamp_parsed", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
            
            # Error monitoring
            error_monitoring = parsed_stream \
                .filter(col("level") == "ERROR") \
                .withWatermark("timestamp_parsed", "2 minutes") \
                .groupBy(
                    window(col("timestamp_parsed"), "2 minutes"),
                    col("server_id"),
                    col("service")
                ) \
                .agg(
                    count("*").alias("error_count"),
                    collect_set("message").alias("error_messages")
                )
            
            query1 = error_monitoring.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 10) \
                .trigger(processingTime="30 seconds") \
                .queryName("error_monitoring_console") \
                .start()
            
            logger.info("✅ Server logs stream started")
            return [query1]
            
        except Exception as e:
            logger.error(f"❌ Error processing server logs: {e}")
            return []
    
    def start_all_streams(self):
        """Start semua streaming queries dengan better error handling"""
        logger.info("🚀 Starting all real-time analytics streams...")
        
        queries = []
        
        try:
            # Test Kafka connectivity first
            test_stream = self.create_kafka_stream("gaming.player.events")
            if not test_stream:
                logger.error("❌ Cannot connect to Kafka. Please ensure Kafka is running and topics exist.")
                return
            
            # Start player events processing
            logger.info("📊 Starting player events processing...")
            player_queries = self.process_player_events_stream()
            if player_queries:
                queries.extend(player_queries)
                logger.info(f"✅ Started {len(player_queries)} player event queries")
            
            # Start reviews processing
            logger.info("📝 Starting reviews processing...")
            review_queries = self.process_game_reviews_stream()
            if review_queries:
                queries.extend(review_queries)
                logger.info(f"✅ Started {len(review_queries)} review queries")
            
            # Start server logs processing
            logger.info("🖥️ Starting server logs processing...")
            log_queries = self.process_server_logs_stream()
            if log_queries:
                queries.extend(log_queries)
                logger.info(f"✅ Started {len(log_queries)} log queries")
            
            if not queries:
                logger.error("❌ No streaming queries started. Check Kafka connection and topics.")
                return
            
            logger.info(f"🎉 Started {len(queries)} streaming queries successfully!")
            logger.info("📊 Real-time analytics is now running. Press Ctrl+C to stop.")
            
            # Wait for all queries
            for query in queries:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping all streams...")
            for query in queries:
                try:
                    query.stop()
                except:
                    pass
            logger.info("✅ All streams stopped")
        
        except Exception as e:
            logger.error(f"❌ Streaming error: {e}")
            for query in queries:
                try:
                    query.stop()
                except:
                    pass

def main():
    """Main function untuk menjalankan real-time analytics"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Real-time Gaming Analytics with Spark Streaming (Fixed)')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--minio-endpoint', default='localhost:9000', help='MinIO endpoint')
    parser.add_argument('--no-minio', action='store_true', help='Disable MinIO integration')
    
    args = parser.parse_args()
    
    # Create and start analytics engine
    try:
        analytics_engine = RealTimeGamingAnalytics(
            kafka_bootstrap_servers=args.kafka_servers,
            minio_endpoint=None if args.no_minio else args.minio_endpoint
        )
        
        analytics_engine.start_all_streams()
        
    except Exception as e:
        logger.error(f"❌ Failed to start real-time analytics: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 