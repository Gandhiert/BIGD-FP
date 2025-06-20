#!/usr/bin/env python3
"""
Fixed Kafka Gaming Events Producer (Python 3.13 Compatible)
Robust producer dengan extensive error handling dan retry logic
"""

import json
import time
import logging
import random
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError
import pandas as pd
from faker import Faker

# Setup comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../logs/kafka_producer_fixed.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RobustGamingEventProducer:
    """
    Enhanced Kafka Producer dengan robust error handling dan retry logic
    """
    
    def __init__(self, 
                 bootstrap_servers='localhost:9092',
                 max_retries=5,
                 retry_backoff_ms=1000):
        """
        Initialize robust Kafka producer
        """
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.fake = Faker()
        self.producer = None
        self.is_running = False
        self.stats = {
            'sent': 0,
            'failed': 0,
            'retries': 0,
            'start_time': None
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"🚀 Initializing Robust Gaming Event Producer")
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("🛑 Shutdown signal received. Stopping producer...")
        self.is_running = False
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close(timeout=5)
                logger.info("✅ Producer shutdown gracefully")
            except Exception as e:
                logger.warning(f"⚠️ Error during shutdown: {e}")
        sys.exit(0)
    
    def _create_producer(self) -> bool:
        """Create Kafka producer dengan retry logic"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"🔄 Attempting to create Kafka producer (attempt {attempt + 1}/{self.max_retries})")
                
                # Conservative configuration untuk stability
                producer_config = {
                    'bootstrap_servers': self.bootstrap_servers,
                    'value_serializer': self._safe_json_serializer,
                    'key_serializer': self._safe_key_serializer,
                    'acks': 1,  # Wait for leader acknowledgment
                    'retries': 3,  # Producer-level retries
                    'retry_backoff_ms': 500,
                    'request_timeout_ms': 30000,  # 30 seconds
                    'delivery_timeout_ms': 120000,  # 2 minutes total
                    'max_in_flight_requests_per_connection': 1,  # Ensure ordering
                    'enable_idempotence': False,  # Disable untuk compatibility
                    'compression_type': 'lz4',  # Fast compression
                    'batch_size': 16384,  # 16KB batches
                    'linger_ms': 100,  # Wait 100ms untuk batching
                    'buffer_memory': 33554432,  # 32MB buffer
                    'max_block_ms': 10000,  # Max 10s block
                    'connections_max_idle_ms': 540000,  # 9 minutes
                    'reconnect_backoff_ms': 50,
                    'reconnect_backoff_max_ms': 1000
                }
                
                self.producer = KafkaProducer(**producer_config)
                
                # Test producer dengan simple message
                test_future = self.producer.send(
                    'gaming.player.events',
                    {'test': True, 'timestamp': datetime.now().isoformat()}
                )
                test_future.get(timeout=10)  # Wait for confirmation
                
                logger.info("✅ Kafka producer created and tested successfully")
                return True
                
            except NoBrokersAvailable:
                logger.error(f"❌ No Kafka brokers available at {self.bootstrap_servers}")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_ms * (2 ** attempt) / 1000
                    logger.info(f"⏳ Waiting {wait_time:.1f}s before retry...")
                    time.sleep(wait_time)
                    
            except KafkaTimeoutError:
                logger.error("❌ Kafka connection timeout")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_ms * (2 ** attempt) / 1000
                    logger.info(f"⏳ Waiting {wait_time:.1f}s before retry...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.error(f"❌ Unexpected error creating producer: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_ms * (2 ** attempt) / 1000
                    logger.info(f"⏳ Waiting {wait_time:.1f}s before retry...")
                    time.sleep(wait_time)
        
        logger.error("❌ Failed to create Kafka producer after all retries")
        return False
    
    def _safe_json_serializer(self, obj):
        """Safe JSON serializer untuk Python 3.13"""
        try:
            # Ensure all values are serializable
            if isinstance(obj, dict):
                cleaned_obj = self._clean_dict_for_serialization(obj)
            else:
                cleaned_obj = obj
                
            return json.dumps(cleaned_obj, default=str, ensure_ascii=False).encode('utf-8')
        except Exception as e:
            logger.error(f"❌ Serialization error: {e}")
            # Return minimal valid JSON
            return json.dumps({
                "error": "serialization_failed",
                "timestamp": datetime.now().isoformat()
            }).encode('utf-8')
    
    def _safe_key_serializer(self, key):
        """Safe key serializer"""
        try:
            if key is None:
                return None
            return str(key).encode('utf-8')
        except Exception:
            return b"unknown_key"
    
    def _clean_dict_for_serialization(self, data: Dict) -> Dict:
        """Clean dictionary untuk JSON serialization"""
        cleaned = {}
        for key, value in data.items():
            try:
                if isinstance(value, (int, float)):
                    # Ensure numeric values are Python native types
                    cleaned[key] = int(value) if isinstance(value, int) and not isinstance(value, bool) else float(value)
                elif isinstance(value, dict):
                    cleaned[key] = self._clean_dict_for_serialization(value)
                elif isinstance(value, (list, tuple)):
                    cleaned[key] = [self._clean_value_for_serialization(v) for v in value]
                elif isinstance(value, str):
                    cleaned[key] = str(value)
                elif isinstance(value, bool):
                    cleaned[key] = bool(value)
                else:
                    cleaned[key] = str(value)
            except Exception as e:
                logger.warning(f"⚠️ Error cleaning key {key}: {e}")
                cleaned[key] = str(value)
        return cleaned
    
    def _clean_value_for_serialization(self, value):
        """Clean individual value untuk serialization"""
        try:
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return int(value) if isinstance(value, int) else float(value)
            elif isinstance(value, dict):
                return self._clean_dict_for_serialization(value)
            elif isinstance(value, bool):
                return bool(value)
            else:
                return str(value)
        except Exception:
            return str(value)
    
    def create_player_event(self) -> Dict:
        """Generate realistic player event dengan safe data types"""
        event_time = datetime.now()
        
        # Ensure all values are safe untuk serialization
        event = {
            'event_id': str(self.fake.uuid4()),
            'player_id': f"player_{random.randint(1, 10000)}",
            'game_id': f"game_{random.randint(1, 500)}",
            'session_id': str(self.fake.uuid4()),
            'event_type': random.choice(['login', 'logout', 'game_start', 'game_end', 'achievement_unlock', 'purchase']),
            'timestamp': event_time.isoformat(),
            'server_region': random.choice(['US-East', 'US-West', 'EU-Central', 'Asia-Pacific']),
            'platform': random.choice(['PC', 'PlayStation', 'Xbox', 'Mobile']),
            'metadata': {
                'level': int(random.randint(1, 100)),
                'score': int(random.randint(0, 999999)),
                'playtime_session': int(random.randint(1, 300)),
                'ip_hash': str(self.fake.sha256()[:16])
            }
        }
        
        return event
    
    def create_game_review(self) -> Dict:
        """Generate realistic game review"""
        review_templates = {
            'positive': [
                "Amazing game! Love the graphics and gameplay.",
                "Best game I've played this year! Highly recommend!",
                "Fantastic experience with great story.",
                "Perfect game with smooth controls.",
                "Incredible graphics and immersive world."
            ],
            'negative': [
                "Buggy mess. Crashes constantly.",
                "Terrible optimization. Very laggy.",
                "Disappointing gameplay. Boring story.",
                "Overpriced for what you get.",
                "Broken mechanics and poor design."
            ],
            'neutral': [
                "Good game but has some issues.",
                "Decent graphics, average story.",
                "It's okay, nothing special.",
                "Some good parts, some bad parts.",
                "Could be better with updates."
            ]
        }
        
        sentiment = random.choice(['positive', 'negative', 'neutral'])
        review_text = random.choice(review_templates[sentiment])
        
        if sentiment == 'positive':
            rating = int(random.randint(8, 10))
        elif sentiment == 'negative':
            rating = int(random.randint(1, 4))
        else:
            rating = int(random.randint(5, 7))
        
        review = {
            'review_id': str(self.fake.uuid4()),
            'player_id': f"player_{random.randint(1, 10000)}",
            'game_id': f"game_{random.randint(1, 500)}",
            'rating': rating,
            'review_text': review_text,
            'timestamp': datetime.now().isoformat(),
            'playtime_hours': int(random.randint(1, 1000)),
            'verified_purchase': bool(random.choice([True, False])),
            'helpful_votes': int(random.randint(0, 100)),
            'total_votes': int(random.randint(0, 150)),
            'sentiment_label': sentiment
        }
        
        return review
    
    def send_event_safely(self, topic: str, event: Dict, key: str = None) -> bool:
        """Send event dengan comprehensive error handling"""
        try:
            if not self.producer:
                logger.error("❌ Producer not initialized")
                return False
            
            # Clean event data
            clean_event = self._clean_dict_for_serialization(event)
            
            # Send asynchronously dengan callback
            future = self.producer.send(
                topic=topic,
                value=clean_event,
                key=key
            )
            
            # Wait for send completion dengan timeout
            record_metadata = future.get(timeout=5)
            
            logger.debug(f"✅ Event sent to {topic}:{record_metadata.partition} at offset {record_metadata.offset}")
            self.stats['sent'] += 1
            return True
            
        except KafkaTimeoutError:
            logger.warning(f"⏰ Timeout sending to {topic}")
            self.stats['failed'] += 1
            return False
            
        except Exception as e:
            logger.error(f"❌ Error sending to {topic}: {e}")
            self.stats['failed'] += 1
            return False
    
    def start_streaming(self, events_per_minute=60, duration_minutes=None):
        """Start streaming dengan robust error handling"""
        
        # Wait for Kafka to be ready
        logger.info("⏳ Waiting for Kafka to be ready...")
        if not self._wait_for_kafka_ready():
            logger.error("❌ Kafka not ready after waiting")
            return
        
        # Create producer
        if not self._create_producer():
            logger.error("❌ Could not create producer")
            return
        
        logger.info(f"🚀 Starting robust event streaming at {events_per_minute} events/minute")
        
        self.is_running = True
        self.stats['start_time'] = time.time()
        
        interval = 60 / events_per_minute  # seconds between events
        last_flush = time.time()
        last_stats = time.time()
        
        try:
            while self.is_running:
                # Check duration limit
                if duration_minutes:
                    elapsed_minutes = (time.time() - self.stats['start_time']) / 60
                    if elapsed_minutes >= duration_minutes:
                        logger.info(f"✅ Completed {duration_minutes} minute run")
                        break
                
                # Generate dan send event
                event_type = random.choices(
                    ['player_event', 'game_review'],
                    weights=[70, 30]  # More player events
                )[0]
                
                success = False
                if event_type == 'player_event':
                    event = self.create_player_event()
                    success = self.send_event_safely(
                        'gaming.player.events',
                        event,
                        key=event['player_id']
                    )
                else:
                    event = self.create_game_review()
                    success = self.send_event_safely(
                        'gaming.reviews.new',
                        event,
                        key=event['review_id']
                    )
                
                # Periodic flush (every 30 seconds)
                current_time = time.time()
                if current_time - last_flush > 30:
                    try:
                        self.producer.flush(timeout=10)
                        last_flush = current_time
                        logger.debug("✅ Periodic flush completed")
                    except Exception as e:
                        logger.warning(f"⚠️ Flush error: {e}")
                
                # Periodic stats (every 2 minutes)
                if current_time - last_stats > 120:
                    self._print_stats()
                    last_stats = current_time
                
                # Control rate
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Streaming stopped by user")
        except Exception as e:
            logger.error(f"❌ Streaming error: {e}")
        finally:
            self._cleanup()
    
    def _wait_for_kafka_ready(self, timeout_seconds=60) -> bool:
        """Wait for Kafka to be ready"""
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            try:
                from kafka.admin import KafkaAdminClient
                admin = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    request_timeout_ms=5000
                )
                
                # Try to get cluster metadata
                admin.describe_cluster()
                admin.close()
                
                logger.info("✅ Kafka is ready")
                return True
                
            except Exception as e:
                logger.debug(f"⏳ Waiting for Kafka... ({e})")
                time.sleep(2)
        
        logger.error("❌ Kafka not ready within timeout")
        return False
    
    def _print_stats(self):
        """Print current statistics"""
        if self.stats['start_time']:
            elapsed = time.time() - self.stats['start_time']
            rate = self.stats['sent'] / elapsed * 60 if elapsed > 0 else 0
            
            logger.info(f"📊 Stats: {self.stats['sent']} sent, {self.stats['failed']} failed, {rate:.1f} events/min")
    
    def _cleanup(self):
        """Cleanup resources"""
        logger.info("🔄 Cleaning up resources...")
        
        if self.producer:
            try:
                logger.info("📤 Flushing remaining messages...")
                self.producer.flush(timeout=15)
                
                logger.info("🔌 Closing producer...")
                self.producer.close(timeout=10)
                
                logger.info("✅ Producer closed successfully")
                
            except Exception as e:
                logger.warning(f"⚠️ Error during cleanup: {e}")
                
        # Final stats
        self._print_stats()
        total_events = self.stats['sent'] + self.stats['failed']
        success_rate = (self.stats['sent'] / total_events * 100) if total_events > 0 else 0
        
        logger.info(f"✅ Final stats: {self.stats['sent']} sent, {self.stats['failed']} failed, {success_rate:.1f}% success rate")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Robust Gaming Events Kafka Producer')
    parser.add_argument('--rate', type=int, default=60, help='Events per minute (default: 60)')
    parser.add_argument('--duration', type=int, help='Duration in minutes (default: infinite)')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    
    args = parser.parse_args()
    
    try:
        producer = RobustGamingEventProducer(bootstrap_servers=args.bootstrap_servers)
        producer.start_streaming(
            events_per_minute=args.rate,
            duration_minutes=args.duration
        )
    except Exception as e:
        logger.error(f"❌ Failed to start producer: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 