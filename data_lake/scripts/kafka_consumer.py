#!/usr/bin/env python3
"""
Gaming Events Kafka Consumer
Consumes streaming data dari Kafka dan processes untuk real-time analytics
"""

import json
import logging
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio
from io import BytesIO
import threading
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GamingEventConsumer:
    """
    Consumer untuk processing gaming events dari Kafka
    """
    
    def __init__(self, 
                 bootstrap_servers='localhost:9092',
                 minio_endpoint='localhost:9000',
                 minio_access_key='minioadmin',
                 minio_secret_key='minioadmin'):
        """
        Initialize Kafka consumer dan MinIO client
        """
        self.bootstrap_servers = bootstrap_servers
        self.running = False
        self.consumers = {}
        self.buffer = {
            'player_events': [],
            'game_reviews': [],
            'server_logs': [],
            'player_stats': [],
            'game_metrics': [],
            'games_stream': []  # New buffer for games streaming data
        }
        
        # MinIO client untuk storage
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        # Define topics to consume
        self.topics = {
            'player_events': 'gaming.player.events',
            'game_reviews': 'gaming.reviews.new',
            'server_logs': 'gaming.server.logs',
            'player_stats': 'gaming.player.stats',
            'game_metrics': 'gaming.metrics.performance',
            'games_stream': 'gaming.games.stream'  # New topic for games CSV streaming
        }
        
        # Buffer settings
        self.buffer_size = 100  # Batch size sebelum flush ke storage
        self.flush_interval = 30  # Flush every 30 seconds
        
        logger.info(f"✅ Kafka Consumer initialized for brokers: {bootstrap_servers}")
    
    def create_consumer(self, topic: str, group_id: str) -> KafkaConsumer:
        """Create Kafka consumer untuk specific topic"""
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',  # Start dari messages terbaru
                enable_auto_commit=True,
                group_id=group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                consumer_timeout_ms=1000  # Timeout untuk polling
            )
            logger.info(f"✅ Created consumer for topic: {topic}")
            return consumer
        except Exception as e:
            logger.error(f"❌ Failed to create consumer for {topic}: {e}")
            return None
    
    def process_player_event(self, event: Dict):
        """Process player event data"""
        # Add processing timestamp
        event['processed_at'] = datetime.now().isoformat()
        
        # Enrich dengan derived fields
        if event.get('event_type') == 'game_start':
            event['session_start_hour'] = datetime.fromisoformat(event['timestamp']).hour
        
        # Add to buffer
        self.buffer['player_events'].append(event)
        
        # Check for real-time alerts
        if event.get('event_type') == 'game_crash':
            logger.warning(f"🚨 Game crash detected for player {event.get('player_id')}")
    
    def process_game_review(self, review: Dict):
        """Process game review dengan sentiment analysis"""
        review['processed_at'] = datetime.now().isoformat()
        
        # Extract sentiment indicators
        text = review.get('review_text', '').lower()
        if any(word in text for word in ['amazing', 'great', 'fantastic', 'love']):
            review['sentiment_indicator'] = 'positive'
        elif any(word in text for word in ['terrible', 'awful', 'hate', 'worst']):
            review['sentiment_indicator'] = 'negative'
        else:
            review['sentiment_indicator'] = 'neutral'
        
        self.buffer['game_reviews'].append(review)
        
        # Alert untuk negative reviews dengan high impact
        if (review.get('sentiment_indicator') == 'negative' and 
            review.get('helpful_votes', 0) > 10):
            logger.warning(f"🚨 High-impact negative review detected for game {review.get('game_id')}")
    
    def process_server_log(self, log: Dict):
        """Process server log entry"""
        log['processed_at'] = datetime.now().isoformat()
        
        # Calculate derived metrics
        if log.get('response_time_ms'):
            if log['response_time_ms'] > 5000:
                log['performance_category'] = 'slow'
            elif log['response_time_ms'] > 1000:
                log['performance_category'] = 'medium'
            else:
                log['performance_category'] = 'fast'
        
        self.buffer['server_logs'].append(log)
        
        # Real-time error monitoring
        if log.get('level') == 'ERROR':
            logger.error(f"🚨 Server error: {log.get('message')} on {log.get('server_id')}")
    
    def process_player_stats(self, stats: Dict):
        """Process player statistics"""
        stats['processed_at'] = datetime.now().isoformat()
        
        # Categorize player type
        playtime = stats.get('total_playtime_hours', 0)
        if playtime > 1000:
            stats['player_category'] = 'hardcore'
        elif playtime > 100:
            stats['player_category'] = 'regular'
        else:
            stats['player_category'] = 'casual'
        
        self.buffer['player_stats'].append(stats)
    
    def process_game_metrics(self, metrics: Dict):
        """Process game performance metrics"""
        metrics['processed_at'] = datetime.now().isoformat()
        
        # Health indicators
        concurrent_players = metrics.get('concurrent_players', 0)
        crash_rate = metrics.get('crash_rate_percent', 0)
        
        if crash_rate > 2.0:
            metrics['health_status'] = 'poor'
        elif crash_rate > 1.0:
            metrics['health_status'] = 'moderate'
        else:
            metrics['health_status'] = 'good'
        
        self.buffer['game_metrics'].append(metrics)
        
        # Alert untuk high crash rates
        if crash_rate > 3.0:
            logger.warning(f"🚨 High crash rate ({crash_rate}%) for game {metrics.get('game_id')}")
    
    def process_games_stream(self, game_data: Dict):
        """Process streaming games data untuk clustering"""
        game_data['processed_at'] = datetime.now().isoformat()
        
        # Validate required fields
        required_fields = ['app_id', 'title', 'price_final', 'positive_ratio', 'user_reviews']
        if not all(field in game_data for field in required_fields):
            logger.warning(f"⚠️ Missing required fields in game data: {game_data.get('app_id', 'unknown')}")
            return
        
        # Add clustering readiness indicators
        game_data['clustering_ready'] = True
        game_data['data_source'] = 'kafka_stream'
        
        # Extract features if available
        if 'features' in game_data:
            game_data['features_extracted'] = True
        
        self.buffer['games_stream'].append(game_data)
        
        # Log high-value games
        if game_data.get('price_final', 0) > 50:
            logger.info(f"💰 High-value game streamed: {game_data.get('title', 'Unknown')} - ${game_data.get('price_final', 0)}")
        
        # Log popular games
        if game_data.get('user_reviews', 0) > 10000:
            logger.info(f"🎮 Popular game streamed: {game_data.get('title', 'Unknown')} - {game_data.get('user_reviews', 0)} reviews")
    
    def flush_buffer_to_minio(self, event_type: str):
        """Flush buffer ke MinIO storage"""
        if not self.buffer[event_type]:
            return
        
        try:
            # Convert buffer to DataFrame
            df = pd.DataFrame(self.buffer[event_type])
            
            # Generate filename dengan timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"streaming/{event_type}/batch_{timestamp}.parquet"
            
            # Convert to Parquet
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Upload ke MinIO
            self.minio_client.put_object(
                bucket_name='streaming-zone',
                object_name=filename,
                data=parquet_buffer,
                length=len(parquet_buffer.getvalue())
            )
            
            logger.info(f"✅ Flushed {len(df)} {event_type} records to MinIO: {filename}")
            
            # Clear buffer
            self.buffer[event_type] = []
            
        except Exception as e:
            logger.error(f"❌ Failed to flush {event_type} to MinIO: {e}")
    
    def flush_all_buffers(self):
        """Flush semua buffers ke storage"""
        for event_type in self.buffer.keys():
            if len(self.buffer[event_type]) > 0:
                self.flush_buffer_to_minio(event_type)
    
    def start_periodic_flush(self):
        """Start background thread untuk periodic flush"""
        def periodic_flush():
            while self.running:
                time.sleep(self.flush_interval)
                if self.running:
                    self.flush_all_buffers()
        
        flush_thread = threading.Thread(target=periodic_flush, daemon=True)
        flush_thread.start()
        logger.info(f"✅ Started periodic flush every {self.flush_interval} seconds")
    
    def consume_topic(self, topic_key: str):
        """Consume messages dari specific topic"""
        topic = self.topics[topic_key]
        consumer = self.create_consumer(topic, f"gaming-analytics-{topic_key}")
        
        if not consumer:
            return
        
        try:
            while self.running:
                message_batch = consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Process berdasarkan topic type
                            if topic_key == 'player_events':
                                self.process_player_event(message.value)
                            elif topic_key == 'game_reviews':
                                self.process_game_review(message.value)
                            elif topic_key == 'server_logs':
                                self.process_server_log(message.value)
                            elif topic_key == 'player_stats':
                                self.process_player_stats(message.value)
                            elif topic_key == 'game_metrics':
                                self.process_game_metrics(message.value)
                            elif topic_key == 'games_stream':
                                self.process_games_stream(message.value)
                            
                            # Check buffer size untuk immediate flush
                            if len(self.buffer[topic_key]) >= self.buffer_size:
                                self.flush_buffer_to_minio(topic_key)
                                
                        except Exception as e:
                            logger.error(f"❌ Error processing message from {topic}: {e}")
                            
        except Exception as e:
            logger.error(f"❌ Consumer error for {topic}: {e}")
        finally:
            consumer.close()
            logger.info(f"✅ Closed consumer for {topic}")
    
    def start_consuming(self):
        """Start consuming dari semua topics"""
        logger.info("🚀 Starting multi-topic consumer...")
        
        # Ensure streaming bucket exists
        try:
            if not self.minio_client.bucket_exists('streaming-zone'):
                self.minio_client.make_bucket('streaming-zone')
                logger.info("✅ Created streaming-zone bucket")
        except Exception as e:
            logger.warning(f"⚠️ Could not create streaming bucket: {e}")
        
        self.running = True
        
        # Start periodic flush
        self.start_periodic_flush()
        
        # Start consumer threads untuk each topic
        consumer_threads = []
        for topic_key in self.topics.keys():
            thread = threading.Thread(
                target=self.consume_topic,
                args=(topic_key,),
                daemon=True
            )
            thread.start()
            consumer_threads.append(thread)
            logger.info(f"✅ Started consumer thread for {topic_key}")
        
        # Setup signal handlers untuk graceful shutdown
        def signal_handler(signum, frame):
            logger.info("🛑 Shutting down consumer...")
            self.running = False
            
            # Flush remaining data
            self.flush_all_buffers()
            
            # Wait for threads to finish
            for thread in consumer_threads:
                thread.join(timeout=5)
            
            logger.info("✅ Consumer shutdown complete")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Keep main thread alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            signal_handler(None, None)

def main():
    """Main function untuk menjalankan consumer"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Gaming Events Kafka Consumer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--minio-endpoint', default='localhost:9000', help='MinIO endpoint')
    parser.add_argument('--buffer-size', type=int, default=100, help='Buffer size before flush')
    parser.add_argument('--flush-interval', type=int, default=30, help='Flush interval in seconds')
    
    args = parser.parse_args()
    
    # Create and start consumer
    consumer = GamingEventConsumer(
        bootstrap_servers=args.bootstrap_servers,
        minio_endpoint=args.minio_endpoint
    )
    
    consumer.buffer_size = args.buffer_size
    consumer.flush_interval = args.flush_interval
    
    consumer.start_consuming()

if __name__ == "__main__":
    main() 