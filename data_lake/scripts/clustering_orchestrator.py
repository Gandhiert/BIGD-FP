#!/usr/bin/env python3
"""
Games Clustering Orchestrator
Mengatur alur kerja end-to-end untuk real-time clustering games
"""

import os
import sys
import time
import logging
import subprocess
import signal
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import json
import requests
from minio import Minio

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../logs/clustering_orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ClusteringOrchestrator:
    """
    Orchestrator untuk mengelola pipeline real-time clustering games
    
    Pipeline Flow:
    1. Start Kafka producer (games CSV streaming)
    2. Start Kafka consumer (save to MinIO)
    3. Monitor data accumulation
    4. Trigger clustering job
    5. Monitor clustering completion
    6. Verify API availability
    """
    
    def __init__(self,
                 kafka_servers="localhost:9092",
                 minio_endpoint="localhost:9000",
                 minio_access_key="minioadmin",
                 minio_secret_key="minioadmin",
                 api_endpoint="http://localhost:5000",
                 clustering_schedule_minutes=30,
                 min_games_threshold=80):
        """
        Initialize Clustering Orchestrator
        
        Args:
            kafka_servers: Kafka bootstrap servers
            minio_endpoint: MinIO endpoint
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
            api_endpoint: Analytics API endpoint
            clustering_schedule_minutes: Minutes between clustering runs
            min_games_threshold: Minimum games required for clustering
        """
        self.kafka_servers = kafka_servers
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.api_endpoint = api_endpoint
        self.clustering_schedule_minutes = clustering_schedule_minutes
        self.min_games_threshold = min_games_threshold
        
        # Process management
        self.producer_process = None
        self.consumer_process = None
        self.api_process = None
        self.is_running = False
        
        # Statistics
        self.stats = {
            'start_time': None,
            'clustering_runs': 0,
            'last_clustering': None,
            'games_processed': 0,
            'errors': 0
        }
        
        # MinIO client
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("🎯 Clustering Orchestrator Initialized")
        logger.info(f"⚙️ Configuration:")
        logger.info(f"   Kafka: {kafka_servers}")
        logger.info(f"   MinIO: {minio_endpoint}")
        logger.info(f"   API: {api_endpoint}")
        logger.info(f"   Clustering interval: {clustering_schedule_minutes} minutes")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("🛑 Shutdown signal received. Stopping orchestrator...")
        self.is_running = False
        self._cleanup_processes()
        self._print_final_stats()
        sys.exit(0)
    
    def _cleanup_processes(self):
        """Cleanup all running processes"""
        processes = [
            ("Producer", self.producer_process),
            ("Consumer", self.consumer_process),
            ("API", self.api_process)
        ]
        
        for name, process in processes:
            if process and process.poll() is None:
                logger.info(f"🛑 Stopping {name} process...")
                try:
                    process.terminate()
                    process.wait(timeout=10)
                    logger.info(f"✅ {name} stopped gracefully")
                except subprocess.TimeoutExpired:
                    logger.warning(f"⚠️ Force killing {name} process...")
                    process.kill()
                    process.wait()
                except Exception as e:
                    logger.error(f"❌ Error stopping {name}: {e}")
    
    def ensure_directories(self):
        """Ensure required directories exist"""
        directories = [
            '../logs',
            '../spark_jobs',
            '../scripts'
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"📁 Directory ensured: {directory}")
    
    def check_dependencies(self) -> bool:
        """Check if all required dependencies are available"""
        logger.info("🔍 Checking dependencies...")
        
        # Check if required scripts exist
        required_scripts = [
            'games_csv_producer.py',
            'kafka_consumer.py',
            'analytics_api.py',
            '../spark_jobs/games_clustering.py'
        ]
        
        for script in required_scripts:
            script_path = os.path.join(os.path.dirname(__file__), script)
            if not os.path.exists(script_path):
                logger.error(f"❌ Required script not found: {script}")
                return False
            logger.info(f"✅ Script found: {script}")
        
        # Check games.csv
        games_csv_path = "../../games.csv"
        if not os.path.exists(games_csv_path):
            logger.error(f"❌ games.csv not found: {games_csv_path}")
            return False
        logger.info(f"✅ games.csv found")
        
        # Check MinIO connectivity
        try:
            # Try to list buckets
            buckets = list(self.minio_client.list_buckets())
            logger.info(f"✅ MinIO connected. Buckets: {[b.name for b in buckets]}")
        except Exception as e:
            logger.error(f"❌ MinIO connection failed: {e}")
            return False
        
        logger.info("✅ All dependencies checked successfully")
        return True
    
    def start_producer(self) -> bool:
        """Start Kafka producer for games CSV streaming"""
        try:
            logger.info("🚀 Starting Games CSV Kafka Producer...")
            
            cmd = [
                sys.executable,
                'games_csv_producer.py',
                '--csv-file', '../../games.csv',
                '--kafka-servers', self.kafka_servers,
                '--topic', 'gaming.games.stream',
                '--batch-size', '50',
                '--delay', '2.0',
                '--shuffle',
                '--loop'
            ]
            
            self.producer_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=os.path.dirname(__file__),
                universal_newlines=True
            )
            
            # Wait a bit and check if process started successfully
            time.sleep(3)
            if self.producer_process.poll() is None:
                logger.info("✅ Producer started successfully")
                return True
            else:
                stdout, _ = self.producer_process.communicate()
                logger.error(f"❌ Producer failed to start: {stdout}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error starting producer: {e}")
            return False
    
    def start_consumer(self) -> bool:
        """Start Kafka consumer"""
        try:
            logger.info("🚀 Starting Kafka Consumer...")
            
            cmd = [
                sys.executable,
                'kafka_consumer.py',
                '--bootstrap-servers', self.kafka_servers,
                '--minio-endpoint', self.minio_endpoint,
                '--buffer-size', '100',
                '--flush-interval', '30'
            ]
            
            self.consumer_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=os.path.dirname(__file__),
                universal_newlines=True
            )
            
            # Wait a bit and check if process started successfully
            time.sleep(3)
            if self.consumer_process.poll() is None:
                logger.info("✅ Consumer started successfully")
                return True
            else:
                stdout, _ = self.consumer_process.communicate()
                logger.error(f"❌ Consumer failed to start: {stdout}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error starting consumer: {e}")
            return False
    
    def start_api(self) -> bool:
        """Start Analytics API"""
        try:
            logger.info("🚀 Starting Analytics API...")
            
            cmd = [
                sys.executable,
                'analytics_api.py'
            ]
            
            self.api_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=os.path.dirname(__file__),
                universal_newlines=True
            )
            
            # Wait longer for API to start and handle Flask debug mode restart
            logger.info("⏳ Waiting for API to fully initialize (including debug mode restart)...")
            time.sleep(15)  # Increased from 5 to 15 seconds
            
            # Test API connectivity with retry mechanism
            max_retries = 5
            retry_delay = 3
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"🔄 Testing API connectivity (attempt {attempt + 1}/{max_retries})...")
                    response = requests.get(f"{self.api_endpoint}/api/health", timeout=20)
                    if response.status_code == 200:
                        logger.info("✅ API started successfully and is responding")
                        return True
                    else:
                        logger.warning(f"⚠️ API responded with status: {response.status_code}")
                        if attempt < max_retries - 1:
                            logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                        continue
                except requests.RequestException as e:
                    logger.warning(f"⚠️ API connectivity test failed (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    continue
            
            logger.error(f"❌ API failed to respond after {max_retries} attempts")
            return False
                
        except Exception as e:
            logger.error(f"❌ Error starting API: {e}")
            return False
    
    def check_streaming_data_availability(self) -> Dict:
        """Check if sufficient streaming data is available for clustering"""
        try:
            # Check streaming bucket for games data
            prefix = "streaming/games_stream/"
            logger.info(f"🔍 Checking data with prefix: {prefix} in bucket: streaming-zone")
            
            objects = list(self.minio_client.list_objects(
                'streaming-zone', 
                prefix=prefix, 
                recursive=True
            ))
            
            # Debug: list first few objects
            if objects:
                logger.info(f"📋 Found {len(objects)} objects, first few:")
                for i, obj in enumerate(objects[:3]):
                    logger.info(f"   {i+1}. {obj.object_name} (size: {obj.size}, modified: {obj.last_modified})")
            else:
                # Try listing all objects in bucket for debugging
                logger.warning("⚠️ No objects found with prefix, checking entire bucket...")
                all_objects = list(self.minio_client.list_objects('streaming-zone', recursive=True))
                if all_objects:
                    logger.info(f"📋 Found {len(all_objects)} total objects in bucket:")
                    for i, obj in enumerate(all_objects[:5]):
                        logger.info(f"   {i+1}. {obj.object_name}")
                else:
                    logger.warning("⚠️ Bucket is completely empty")
            
            total_files = len(objects)
            # Fix timezone issue: make datetime.now() timezone-aware for comparison
            now_utc = datetime.now(timezone.utc)
            one_hour_ago = now_utc - timedelta(hours=1)
            recent_files = len([
                obj for obj in objects 
                if obj.last_modified > one_hour_ago
            ])
            
            # Estimate games count (rough estimation)
            estimated_games = total_files * 50  # Assuming 50 games per batch file
            
            status = {
                'total_files': total_files,
                'recent_files_1h': recent_files,
                'estimated_games': estimated_games,
                'sufficient_data': estimated_games >= self.min_games_threshold,
                'last_activity': max([obj.last_modified for obj in objects]).isoformat() if objects else None,
                'prefix_used': prefix
            }
            
            logger.info(f"📊 Streaming data check: {status}")
            return status
            
        except Exception as e:
            logger.error(f"❌ Error checking streaming data: {e}")
            return {
                'error': str(e),
                'sufficient_data': False,
                'estimated_games': 0,
                'prefix_used': prefix
            }
    
    def trigger_clustering_job(self) -> bool:
        """Trigger PySpark clustering job"""
        try:
            logger.info("🎯 Triggering clustering job...")
            
            cmd = [
                sys.executable,
                '../spark_jobs/games_clustering.py',
                '--minio-endpoint', self.minio_endpoint,
                '--minio-access-key', self.minio_access_key,
                '--minio-secret-key', self.minio_secret_key,
                '--input-bucket', 'streaming-zone',
                '--output-bucket', 'clusters-zone',
                '--k-clusters', '13',
                '--hours-back', '24'
            ]
            
            # Run clustering job and wait for completion
            clustering_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=os.path.dirname(__file__),
                universal_newlines=True
            )
            
            logger.info("⏳ Waiting for clustering job to complete...")
            stdout, _ = clustering_process.communicate()
            
            if clustering_process.returncode == 0:
                logger.info("✅ Clustering job completed successfully")
                self.stats['clustering_runs'] += 1
                self.stats['last_clustering'] = datetime.now().isoformat()
                return True
            else:
                logger.error(f"❌ Clustering job failed: {stdout}")
                self.stats['errors'] += 1
                return False
                
        except Exception as e:
            logger.error(f"❌ Error triggering clustering job: {e}")
            self.stats['errors'] += 1
            return False
    
    def verify_clustering_results(self) -> bool:
        """Verify that clustering results are available via API"""
        try:
            logger.info("🔍 Verifying clustering results...")
            
            # Add delay to allow MinIO to properly update file timestamps
            logger.info("⏳ Waiting 10 seconds for file system consistency...")
            time.sleep(10)
            
            # Check clustering status via API with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = requests.get(f"{self.api_endpoint}/api/clustering/status", timeout=30)
                    if response.status_code == 200:
                        status_data = response.json()
                        clustering_status = status_data.get('clustering_status', 'unknown')
                        
                        # Log detailed status for debugging
                        logger.info(f"📊 Clustering status response: {status_data}")
                        
                        if clustering_status == 'healthy':
                            logger.info("✅ Clustering results verified successfully")
                            
                            # Test getting actual clustered games
                            games_response = requests.get(
                                f"{self.api_endpoint}/api/clustering/games?limit=10", 
                                timeout=30
                            )
                            
                            if games_response.status_code == 200:
                                games_data = games_response.json()
                                game_count = games_data.get('pagination', {}).get('total', 0)
                                self.stats['games_processed'] = game_count
                                logger.info(f"✅ API verification successful. Total games: {game_count}")
                                return True
                            else:
                                logger.error(f"❌ Games API test failed: {games_response.status_code}")
                                return False
                        elif clustering_status == 'stale':
                            # If stale on first attempt, retry after a longer delay
                            if attempt < max_retries - 1:
                                logger.warning(f"⚠️ Clustering status: {clustering_status}, retrying in 15 seconds... (attempt {attempt + 1}/{max_retries})")
                                time.sleep(15)
                                continue
                            else:
                                logger.warning(f"⚠️ Final attempt - Clustering status: {clustering_status}")
                                # Accept stale results if files exist (better than no verification)
                                if status_data.get('latest_clusters') and status_data.get('latest_analysis'):
                                    logger.info("✅ Accepting stale results - files exist and clustering completed")
                                    return True
                                return False
                        else:
                            logger.warning(f"⚠️ Clustering status: {clustering_status}")
                            if attempt < max_retries - 1:
                                logger.info(f"🔄 Retrying verification in 10 seconds... (attempt {attempt + 1}/{max_retries})")
                                time.sleep(10)
                                continue
                            return False
                    else:
                        logger.error(f"❌ Clustering status API failed: {response.status_code}")
                        if attempt < max_retries - 1:
                            logger.info(f"🔄 Retrying verification in 10 seconds... (attempt {attempt + 1}/{max_retries})")
                            time.sleep(10)
                            continue
                        return False
                except requests.RequestException as e:
                    logger.error(f"❌ Request error (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"🔄 Retrying verification in 10 seconds...")
                        time.sleep(10)
                        continue
                    return False
            
            return False
                
        except Exception as e:
            logger.error(f"❌ Error verifying clustering results: {e}")
            return False
    
    def monitor_system_health(self) -> Dict:
        """Monitor overall system health"""
        health = {
            'timestamp': datetime.now().isoformat(),
            'producer_status': 'running' if self.producer_process and self.producer_process.poll() is None else 'stopped',
            'consumer_status': 'running' if self.consumer_process and self.consumer_process.poll() is None else 'stopped',
            'api_status': 'unknown',
            'streaming_data': {},
            'clustering_status': 'unknown'
        }
        
        # Check API health
        try:
            response = requests.get(f"{self.api_endpoint}/api/health", timeout=10)
            health['api_status'] = 'healthy' if response.status_code == 200 else 'unhealthy'
        except:
            health['api_status'] = 'unreachable'
        
        # Check streaming data
        health['streaming_data'] = self.check_streaming_data_availability()
        
        # Check clustering status
        try:
            response = requests.get(f"{self.api_endpoint}/api/clustering/status", timeout=10)
            if response.status_code == 200:
                health['clustering_status'] = response.json().get('clustering_status', 'unknown')
        except:
            health['clustering_status'] = 'unreachable'
        
        return health
    
    def run_orchestrator(self):
        """Run the main orchestration loop"""
        logger.info("🚀 Starting Clustering Orchestrator")
        logger.info("=" * 60)
        
        self.stats['start_time'] = datetime.now()
        self.is_running = True
        
        try:
            # Pre-flight checks
            self.ensure_directories()
            if not self.check_dependencies():
                logger.error("❌ Dependency check failed. Exiting.")
                return False
            
            # Start core components
            logger.info("🔧 Starting core components...")
            
            if not self.start_producer():
                logger.error("❌ Failed to start producer. Exiting.")
                return False
            
            if not self.start_consumer():
                logger.error("❌ Failed to start consumer. Exiting.")
                return False
            
            if not self.start_api():
                logger.error("❌ Failed to start API. Exiting.")
                return False
            
            logger.info("✅ All core components started successfully")
            
            # Wait for initial data accumulation
            logger.info(f"⏳ Waiting for initial data accumulation (3 minutes)...")
            time.sleep(180)  # 5 minutes
            
            # Main orchestration loop
            last_clustering = datetime.now() - timedelta(hours=1)  # Force initial clustering
            
            while self.is_running:
                try:
                    # Monitor system health
                    health = self.monitor_system_health()
                    logger.info(f"💊 System Health: Producer={health['producer_status']}, "
                               f"Consumer={health['consumer_status']}, API={health['api_status']}, "
                               f"Clustering={health['clustering_status']}")
                    
                    # Check if it's time for clustering
                    time_since_last_clustering = datetime.now() - last_clustering
                    should_cluster = time_since_last_clustering.total_seconds() >= (self.clustering_schedule_minutes * 60)
                    
                    if should_cluster:
                        # Check if we have sufficient data
                        data_status = self.check_streaming_data_availability()
                        
                        if data_status.get('sufficient_data', False):
                            logger.info("🎯 Conditions met for clustering. Starting clustering job...")
                            
                            if self.trigger_clustering_job():
                                # Verify results
                                if self.verify_clustering_results():
                                    logger.info("🎉 Clustering cycle completed successfully!")
                                    last_clustering = datetime.now()
                                else:
                                    logger.error("❌ Clustering verification failed")
                            else:
                                logger.error("❌ Clustering job failed")
                        else:
                            logger.info(f"⏳ Insufficient data for clustering. "
                                       f"Games: {data_status.get('estimated_games', 0)}, "
                                       f"Required: {self.min_games_threshold}")
                    
                    # Restart failed components
                    if health['producer_status'] == 'stopped':
                        logger.warning("⚠️ Producer stopped. Restarting...")
                        self.start_producer()
                    
                    if health['consumer_status'] == 'stopped':
                        logger.warning("⚠️ Consumer stopped. Restarting...")
                        self.start_consumer()
                    
                    if health['api_status'] == 'unreachable':
                        logger.warning("⚠️ API unreachable. Restarting...")
                        self.start_api()
                    
                    # Print stats periodically
                    if self.stats['clustering_runs'] % 5 == 0 and self.stats['clustering_runs'] > 0:
                        self._print_stats()
                    
                    # Sleep before next check
                    time.sleep(60)  # Check every minute
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"❌ Error in orchestration loop: {e}")
                    self.stats['errors'] += 1
                    time.sleep(30)  # Wait before retrying
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Orchestrator failed: {e}")
            return False
        finally:
            self._cleanup_processes()
    
    def _print_stats(self):
        """Print orchestrator statistics"""
        if self.stats['start_time']:
            elapsed = datetime.now() - self.stats['start_time']
            
            logger.info("📊 ORCHESTRATOR STATISTICS")
            logger.info("=" * 40)
            logger.info(f"⏱️ Runtime: {elapsed}")
            logger.info(f"🎯 Clustering runs: {self.stats['clustering_runs']}")
            logger.info(f"🎮 Games processed: {self.stats['games_processed']:,}")
            logger.info(f"❌ Errors: {self.stats['errors']}")
            logger.info(f"📈 Last clustering: {self.stats['last_clustering']}")
    
    def _print_final_stats(self):
        """Print final statistics"""
        logger.info("📊 FINAL ORCHESTRATOR STATISTICS")
        logger.info("=" * 50)
        self._print_stats()
        logger.info("✅ Orchestrator shutdown complete")


def main():
    """Main function untuk menjalankan orchestrator"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Games Clustering Orchestrator')
    parser.add_argument('--kafka-servers', default='localhost:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--minio-endpoint', default='localhost:9000',
                       help='MinIO endpoint')
    parser.add_argument('--minio-access-key', default='minioadmin',
                       help='MinIO access key')
    parser.add_argument('--minio-secret-key', default='minioadmin',
                       help='MinIO secret key')
    parser.add_argument('--api-endpoint', default='http://localhost:5000',
                       help='Analytics API endpoint')
    parser.add_argument('--clustering-interval', type=int, default=30,
                       help='Minutes between clustering runs')
    parser.add_argument('--min-games', type=int, default=80,
                       help='Minimum games threshold for clustering')
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = ClusteringOrchestrator(
        kafka_servers=args.kafka_servers,
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
        api_endpoint=args.api_endpoint,
        clustering_schedule_minutes=args.clustering_interval,
        min_games_threshold=args.min_games
    )
    
    logger.info("🎮 Starting Games Clustering Orchestrator")
    logger.info(f"⚙️ Configuration:")
    logger.info(f"   Kafka: {args.kafka_servers}")
    logger.info(f"   MinIO: {args.minio_endpoint}")
    logger.info(f"   API: {args.api_endpoint}")
    logger.info(f"   Clustering interval: {args.clustering_interval} minutes")
    logger.info(f"   Min games threshold: {args.min_games}")
    
    try:
        success = orchestrator.run_orchestrator()
        if success:
            logger.info("✅ Orchestrator completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Orchestrator failed")
            sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Orchestrator error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()