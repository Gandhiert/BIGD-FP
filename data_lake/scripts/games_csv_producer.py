#!/usr/bin/env python3
"""
Games CSV Real-time Kafka Producer
Producer khusus untuk streaming data games.csv ke Kafka secara real-time
"""

import json
import time
import logging
import pandas as pd
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError
import numpy as np

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../logs/games_csv_producer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GamesCsvKafkaProducer:
    """
    Producer untuk streaming data games.csv ke Kafka secara real-time
    """
    
    def __init__(self, 
                 csv_file_path='../../games.csv',
                 bootstrap_servers='localhost:9092',
                 topic='gaming.games.stream',
                 batch_size=100,
                 streaming_delay=1.0):
        """
        Initialize Games CSV Kafka Producer
        
        Args:
            csv_file_path: Path ke file games.csv
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic untuk streaming
            batch_size: Jumlah records per batch
            streaming_delay: Delay antar batch (seconds)
        """
        self.csv_file_path = csv_file_path
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.batch_size = batch_size
        self.streaming_delay = streaming_delay
        self.producer = None
        self.is_running = False
        
        # Statistics
        self.stats = {
            'total_sent': 0,
            'total_failed': 0,
            'batches_sent': 0,
            'start_time': None,
            'current_batch': 0
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"🎮 Initializing Games CSV Kafka Producer")
        logger.info(f"📂 CSV File: {csv_file_path}")
        logger.info(f"📡 Kafka Topic: {topic}")
        logger.info(f"📦 Batch Size: {batch_size}")
        
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
        self._print_final_stats()
        sys.exit(0)
    
    def _create_producer(self) -> bool:
        """Create Kafka producer dengan konfigurasi optimal"""
        try:
            logger.info("🔄 Creating Kafka producer...")
            
            producer_config = {
                'bootstrap_servers': self.bootstrap_servers,
                'value_serializer': self._json_serializer,
                'key_serializer': lambda x: str(x).encode('utf-8') if x else None,
                'acks': 1,
                'retries': 3,
                'retry_backoff_ms': 500,
                'request_timeout_ms': 30000,
                'compression_type': 'lz4',
                'batch_size': 16384,
                'linger_ms': 100,
                'buffer_memory': 33554432
            }
            
            self.producer = KafkaProducer(**producer_config)
            
            # Test connection
            test_future = self.producer.send(
                self.topic,
                {'test': True, 'timestamp': datetime.now().isoformat()}
            )
            test_future.get(timeout=10)
            
            logger.info("✅ Kafka producer created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create Kafka producer: {e}")
            return False
    
    def _json_serializer(self, obj):
        """Safe JSON serializer"""
        try:
            # Handle pandas/numpy types
            if isinstance(obj, dict):
                cleaned_obj = self._clean_dict_for_json(obj)
            else:
                cleaned_obj = obj
                
            return json.dumps(cleaned_obj, default=str, ensure_ascii=False).encode('utf-8')
        except Exception as e:
            logger.error(f"❌ Serialization error: {e}")
            return json.dumps({
                "error": "serialization_failed",
                "timestamp": datetime.now().isoformat()
            }).encode('utf-8')
    
    def _clean_dict_for_json(self, data: Dict) -> Dict:
        """Clean dictionary untuk JSON serialization"""
        cleaned = {}
        for key, value in data.items():
            try:
                if pd.isna(value):
                    cleaned[key] = None
                elif isinstance(value, (np.integer, np.int64)):
                    cleaned[key] = int(value)
                elif isinstance(value, (np.floating, np.float64)):
                    cleaned[key] = float(value)
                elif isinstance(value, np.bool_):
                    cleaned[key] = bool(value)
                elif isinstance(value, (int, float, str, bool)):
                    cleaned[key] = value
                elif isinstance(value, dict):
                    cleaned[key] = self._clean_dict_for_json(value)
                elif isinstance(value, (list, tuple)):
                    cleaned[key] = [self._clean_value_for_json(v) for v in value]
                else:
                    cleaned[key] = str(value)
            except Exception as e:
                logger.warning(f"⚠️ Error cleaning key {key}: {e}")
                cleaned[key] = str(value) if value is not None else None
        return cleaned
    
    def _clean_value_for_json(self, value):
        """Clean individual value untuk JSON"""
        try:
            if pd.isna(value):
                return None
            elif isinstance(value, (np.integer, np.int64)):
                return int(value)
            elif isinstance(value, (np.floating, np.float64)):
                return float(value)
            elif isinstance(value, np.bool_):
                return bool(value)
            elif isinstance(value, (int, float, str, bool)):
                return value
            else:
                return str(value)
        except Exception:
            return str(value) if value is not None else None
    
    def load_games_data(self) -> Optional[pd.DataFrame]:
        """Load games.csv data"""
        try:
            logger.info(f"📂 Loading games data from {self.csv_file_path}")
            df = pd.read_csv(self.csv_file_path)
            
            logger.info(f"✅ Loaded {len(df):,} games from CSV")
            logger.info(f"📊 Columns: {list(df.columns)}")
            
            return df
            
        except FileNotFoundError:
            logger.error(f"❌ Games CSV file not found: {self.csv_file_path}")
            return None
        except Exception as e:
            logger.error(f"❌ Error loading games data: {e}")
            return None
    
    def preprocess_game_record(self, row: pd.Series) -> Dict:
        """Preprocess single game record untuk streaming"""
        try:
            # Convert pandas Series to dict
            game_data = row.to_dict()
            
            # Add streaming metadata
            game_data['streaming_metadata'] = {
                'timestamp': datetime.now().isoformat(),
                'source': 'games_csv_producer',
                'batch_id': self.stats['current_batch'],
                'record_id': f"game_{row.get('app_id', 'unknown')}_{int(time.time())}"
            }
            
            # Add feature engineering fields untuk clustering
            game_data['features'] = {
                'platform_count': int(row.get('win', False)) + int(row.get('mac', False)) + int(row.get('linux', False)),
                'price_category': self._categorize_price(row.get('price_final', 0)),
                'review_category': self._categorize_reviews(row.get('user_reviews', 0)),
                'rating_score': self._encode_rating(row.get('rating', 'Unknown'))
            }
            
            return game_data
            
        except Exception as e:
            logger.error(f"❌ Error preprocessing game record: {e}")
            return {
                'error': 'preprocessing_failed',
                'raw_data': str(row.to_dict()),
                'timestamp': datetime.now().isoformat()
            }
    
    def _categorize_price(self, price: float) -> int:
        """Kategorikan harga game"""
        try:
            price = float(price)
            if price == 0:
                return 0  # Free
            elif price <= 5:
                return 1  # Budget
            elif price <= 20:
                return 2  # Mid-range
            elif price <= 50:
                return 3  # Premium
            else:
                return 4  # Expensive
        except:
            return 0
    
    def _categorize_reviews(self, reviews: int) -> int:
        """Kategorikan jumlah reviews"""
        try:
            reviews = int(reviews)
            if reviews < 100:
                return 0  # Few
            elif reviews < 1000:
                return 1  # Moderate
            elif reviews < 10000:
                return 2  # Popular
            else:
                return 3  # Very Popular
        except:
            return 0
    
    def _encode_rating(self, rating: str) -> int:
        """Encode rating string ke numeric"""
        rating_map = {
            'Overwhelmingly Positive': 9,
            'Very Positive': 8,
            'Positive': 6,
            'Mostly Positive': 7,
            'Mixed': 5,
            'Mostly Negative': 3,
            'Negative': 2,
            'Very Negative': 1,
            'Overwhelmingly Negative': 0
        }
        return rating_map.get(str(rating), 4)  # Default to middle value
    
    def send_game_batch(self, games_batch: List[Dict]) -> bool:
        """Send batch of games ke Kafka"""
        success_count = 0
        fail_count = 0
        
        for game_data in games_batch:
            try:
                # Use app_id as key untuk partitioning
                key = str(game_data.get('app_id', f"unknown_{int(time.time())}"))
                
                future = self.producer.send(
                    self.topic,
                    value=game_data,
                    key=key
                )
                
                # Don't wait for individual confirmations for better performance
                success_count += 1
                
            except Exception as e:
                logger.error(f"❌ Failed to send game {game_data.get('app_id', 'unknown')}: {e}")
                fail_count += 1
        
        # Flush batch
        try:
            self.producer.flush(timeout=30)
        except Exception as e:
            logger.error(f"❌ Batch flush failed: {e}")
            fail_count += len(games_batch) - success_count
            success_count = 0
        
        # Update statistics
        self.stats['total_sent'] += success_count
        self.stats['total_failed'] += fail_count
        self.stats['batches_sent'] += 1
        
        logger.info(f"📦 Batch {self.stats['current_batch']}: {success_count} sent, {fail_count} failed")
        
        return success_count > 0
    
    def start_streaming(self, shuffle_data=True, loop_streaming=False):
        """
        Start streaming games data ke Kafka
        
        Args:
            shuffle_data: Shuffle data untuk simulasi real-time yang lebih realistis
            loop_streaming: Loop streaming setelah selesai dengan seluruh dataset
        """
        if not self._create_producer():
            logger.error("❌ Cannot start streaming - producer creation failed")
            return
        
        # Load games data
        games_df = self.load_games_data()
        if games_df is None:
            logger.error("❌ Cannot start streaming - data loading failed")
            return
        
        # Shuffle data if requested
        if shuffle_data:
            games_df = games_df.sample(frac=1).reset_index(drop=True)
            logger.info("🔀 Data shuffled for realistic streaming simulation")
        
        self.is_running = True
        self.stats['start_time'] = datetime.now()
        
        logger.info(f"🚀 Starting real-time streaming...")
        logger.info(f"📊 Total games to stream: {len(games_df):,}")
        logger.info(f"📦 Batch size: {self.batch_size}")
        logger.info(f"⏱️ Streaming delay: {self.streaming_delay}s")
        
        try:
            while self.is_running:
                # Process data in batches
                for start_idx in range(0, len(games_df), self.batch_size):
                    if not self.is_running:
                        break
                    
                    end_idx = min(start_idx + self.batch_size, len(games_df))
                    batch_df = games_df.iloc[start_idx:end_idx]
                    
                    # Preprocess batch
                    games_batch = []
                    for _, row in batch_df.iterrows():
                        game_data = self.preprocess_game_record(row)
                        games_batch.append(game_data)
                    
                    # Send batch
                    self.stats['current_batch'] += 1
                    if self.send_game_batch(games_batch):
                        self._print_progress()
                    
                    # Wait before next batch
                    if self.is_running:
                        time.sleep(self.streaming_delay)
                
                # Check if we should loop
                if loop_streaming and self.is_running:
                    logger.info("🔄 Restarting streaming loop...")
                    if shuffle_data:
                        games_df = games_df.sample(frac=1).reset_index(drop=True)
                else:
                    break
                    
        except KeyboardInterrupt:
            logger.info("⏹️ Streaming interrupted by user")
        except Exception as e:
            logger.error(f"❌ Streaming error: {e}")
        finally:
            self.is_running = False
            self._cleanup()
    
    def _print_progress(self):
        """Print streaming progress"""
        if self.stats['batches_sent'] % 10 == 0:  # Print every 10 batches
            elapsed = datetime.now() - self.stats['start_time']
            rate = self.stats['total_sent'] / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
            
            logger.info(f"📈 Progress: {self.stats['total_sent']:,} games sent, "
                       f"{self.stats['batches_sent']} batches, "
                       f"{rate:.1f} games/sec")
    
    def _print_final_stats(self):
        """Print final statistics"""
        if self.stats['start_time']:
            elapsed = datetime.now() - self.stats['start_time']
            avg_rate = self.stats['total_sent'] / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
            
            logger.info("📊 FINAL STREAMING STATISTICS")
            logger.info("=" * 50)
            logger.info(f"✅ Total games sent: {self.stats['total_sent']:,}")
            logger.info(f"❌ Total failed: {self.stats['total_failed']:,}")
            logger.info(f"📦 Total batches: {self.stats['batches_sent']:,}")
            logger.info(f"⏱️ Total time: {elapsed}")
            logger.info(f"📈 Average rate: {avg_rate:.1f} games/sec")
            logger.info(f"✨ Success rate: {(self.stats['total_sent']/(self.stats['total_sent']+self.stats['total_failed'])*100):.1f}%")
    
    def _cleanup(self):
        """Cleanup resources"""
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close(timeout=5)
                logger.info("✅ Producer cleaned up successfully")
            except Exception as e:
                logger.warning(f"⚠️ Cleanup error: {e}")


def main():
    """Main function untuk menjalankan streaming"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Games CSV Kafka Producer')
    parser.add_argument('--csv-file', default='../../games.csv', 
                       help='Path to games.csv file')
    parser.add_argument('--kafka-servers', default='localhost:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='gaming.games.stream',
                       help='Kafka topic')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for streaming')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between batches (seconds)')
    parser.add_argument('--shuffle', action='store_true',
                       help='Shuffle data for realistic streaming')
    parser.add_argument('--loop', action='store_true',
                       help='Loop streaming continuously')
    
    args = parser.parse_args()
    
    # Create and start producer
    producer = GamesCsvKafkaProducer(
        csv_file_path=args.csv_file,
        bootstrap_servers=args.kafka_servers,
        topic=args.topic,
        batch_size=args.batch_size,
        streaming_delay=args.delay
    )
    
    logger.info("🎮 Starting Games CSV Kafka Producer")
    logger.info(f"📂 CSV File: {args.csv_file}")
    logger.info(f"📡 Kafka: {args.kafka_servers}")
    logger.info(f"🎯 Topic: {args.topic}")
    
    try:
        producer.start_streaming(
            shuffle_data=args.shuffle,
            loop_streaming=args.loop
        )
    except Exception as e:
        logger.error(f"❌ Producer failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()