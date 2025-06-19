#!/usr/bin/env python3
"""
Data Lakehouse Orchestrator
Mengkoordinasi semua pipeline ETL dan monitoring dengan scheduling otomatis
"""

import os
import time
import logging
import schedule
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import pandas as pd
from minio import Minio
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../logs/orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataLakehouseOrchestrator:
    """
    Orchestrator untuk mengelola semua aspek data lakehouse:
    - ETL Pipeline scheduling
    - Data quality monitoring
    - Health checks
    - Performance tracking
    """
    
    def __init__(self):
        self.config = self._load_config()
        self.minio_client = self._init_minio()
        self.job_history = []
        self.metrics = {}
        
        # Ensure log directory exists
        os.makedirs('../logs', exist_ok=True)
        
    def _load_config(self) -> Dict:
        """Load orchestrator configuration"""
        return {
            'minio': {
                'endpoint': 'localhost:9000',
                'access_key': 'minioadmin',
                'secret_key': 'minioadmin',
                'secure': False
            },
            'schedules': {
                'full_etl': '05:00',  # Daily at 5 AM
                'incremental_etl': '*/30 * * * *',  # Every 30 minutes
                'data_quality_check': '10:00',  # Daily at 10 AM
                'health_check': '*/5 * * * *',  # Every 5 minutes
                'cleanup': '02:00'  # Daily at 2 AM
            },
            'retention': {
                'raw_data_days': 90,
                'log_files_days': 30,
                'metrics_days': 365
            }
        }
    
    def _init_minio(self) -> Minio:
        """Initialize MinIO client"""
        return Minio(
            self.config['minio']['endpoint'],
            access_key=self.config['minio']['access_key'],
            secret_key=self.config['minio']['secret_key'],
            secure=self.config['minio']['secure']
        )
    
    def run_etl_pipeline(self, pipeline_type: str = 'full') -> Dict:
        """
        Menjalankan ETL pipeline dengan monitoring
        
        Args:
            pipeline_type: 'full' atau 'incremental'
        
        Returns:
            Dict dengan hasil eksekusi pipeline
        """
        start_time = datetime.now()
        job_id = f"etl_{pipeline_type}_{start_time.strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"🚀 Starting {pipeline_type} ETL pipeline (Job ID: {job_id})")
        
        try:
            # Step 1: Upload raw data
            logger.info("📤 Step 1: Uploading raw data to MinIO...")
            upload_result = subprocess.run([
                'python', 'upload_to_minio.py'
            ], capture_output=True, text=True, cwd='.')
            
            if upload_result.returncode != 0:
                raise Exception(f"Upload failed: {upload_result.stderr}")
            
            # Step 2: Download and transform
            logger.info("🔄 Step 2: Running ETL transformations...")
            etl_result = subprocess.run([
                'python', 'download_from_minio_and_transform.py'
            ], capture_output=True, text=True, cwd='.')
            
            if etl_result.returncode != 0:
                raise Exception(f"ETL failed: {etl_result.stderr}")
            
            # Step 3: Upload warehouse data
            logger.info("📤 Step 3: Uploading warehouse data...")
            warehouse_result = subprocess.run([
                'python', 'upload_warehouse_to_minio.py'
            ], capture_output=True, text=True, cwd='.')
            
            if warehouse_result.returncode != 0:
                raise Exception(f"Warehouse upload failed: {warehouse_result.stderr}")
            
            # Calculate metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Get data stats
            stats = self._get_pipeline_stats()
            
            job_result = {
                'job_id': job_id,
                'pipeline_type': pipeline_type,
                'status': 'SUCCESS',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'records_processed': stats.get('total_records', 0),
                'files_processed': stats.get('total_files', 0),
                'data_size_mb': stats.get('total_size_mb', 0)
            }
            
            logger.info(f"✅ ETL pipeline completed successfully in {duration:.2f}s")
            logger.info(f"📊 Processed {stats.get('total_records', 0)} records across {stats.get('total_files', 0)} files")
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            job_result = {
                'job_id': job_id,
                'pipeline_type': pipeline_type,
                'status': 'FAILED',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'error': str(e)
            }
            
            logger.error(f"❌ ETL pipeline failed: {e}")
        
        # Store job history
        self.job_history.append(job_result)
        self._save_job_history()
        
        return job_result
    
    def _get_pipeline_stats(self) -> Dict:
        """Get statistics dari pipeline execution"""
        try:
            stats = {'total_records': 0, 'total_files': 0, 'total_size_mb': 0}
            
            # Get warehouse bucket stats
            objects = self.minio_client.list_objects('warehouse-zone', recursive=True)
            
            for obj in objects:
                stats['total_files'] += 1
                stats['total_size_mb'] += obj.size / (1024 * 1024)
                
                # Estimate records for parquet files
                if obj.object_name.endswith('.parquet'):
                    # Rough estimate: parquet files ~1KB per record on average
                    estimated_records = max(1, int(obj.size / 1024))
                    stats['total_records'] += estimated_records
            
            return stats
            
        except Exception as e:
            logger.warning(f"Could not get pipeline stats: {e}")
            return {'total_records': 0, 'total_files': 0, 'total_size_mb': 0}
    
    def run_data_quality_checks(self) -> Dict:
        """
        Menjalankan data quality checks pada warehouse data
        """
        logger.info("🔍 Running data quality checks...")
        
        quality_report = {
            'timestamp': datetime.now().isoformat(),
            'checks': {},
            'overall_score': 0,
            'issues': []
        }
        
        try:
            # Check 1: File completeness
            expected_files = ['reviews.parquet', 'games.parquet', 'logs.parquet', 'configs.parquet', 'catalog.parquet']
            existing_files = []
            
            objects = self.minio_client.list_objects('warehouse-zone')
            for obj in objects:
                existing_files.append(obj.object_name)
            
            missing_files = set(expected_files) - set(existing_files)
            quality_report['checks']['file_completeness'] = {
                'score': (len(expected_files) - len(missing_files)) / len(expected_files) * 100,
                'missing_files': list(missing_files)
            }
            
            if missing_files:
                quality_report['issues'].append(f"Missing files: {missing_files}")
            
            # Check 2: Data freshness (files updated in last 24 hours)
            fresh_files = 0
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            for obj in objects:
                if obj.last_modified > cutoff_time:
                    fresh_files += 1
            
            freshness_score = (fresh_files / len(existing_files) * 100) if existing_files else 0
            quality_report['checks']['data_freshness'] = {
                'score': freshness_score,
                'fresh_files': fresh_files,
                'total_files': len(existing_files)
            }
            
            if freshness_score < 50:
                quality_report['issues'].append("Data freshness below 50%")
            
            # Check 3: File size consistency (detect very small or corrupted files)
            size_issues = []
            for obj in objects:
                if obj.size < 1024:  # Files smaller than 1KB might be corrupted
                    size_issues.append(obj.object_name)
            
            size_score = (len(existing_files) - len(size_issues)) / len(existing_files) * 100 if existing_files else 100
            quality_report['checks']['file_size_consistency'] = {
                'score': size_score,
                'problematic_files': size_issues
            }
            
            if size_issues:
                quality_report['issues'].append(f"Suspiciously small files: {size_issues}")
            
            # Calculate overall score
            scores = [check['score'] for check in quality_report['checks'].values()]
            quality_report['overall_score'] = sum(scores) / len(scores) if scores else 0
            
            # Log results
            if quality_report['overall_score'] >= 90:
                logger.info(f"✅ Data quality excellent: {quality_report['overall_score']:.1f}%")
            elif quality_report['overall_score'] >= 70:
                logger.warning(f"⚠️ Data quality needs attention: {quality_report['overall_score']:.1f}%")
            else:
                logger.error(f"❌ Data quality critical: {quality_report['overall_score']:.1f}%")
            
            for issue in quality_report['issues']:
                logger.warning(f"🔍 Quality Issue: {issue}")
                
        except Exception as e:
            logger.error(f"❌ Data quality check failed: {e}")
            quality_report['error'] = str(e)
        
        # Save quality report
        self._save_quality_report(quality_report)
        
        return quality_report
    
    def run_health_check(self) -> Dict:
        """
        Comprehensive health check untuk seluruh sistem
        """
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'components': {},
            'overall_status': 'HEALTHY'
        }
        
        # Check MinIO connectivity
        try:
            buckets = self.minio_client.list_buckets()
            health_status['components']['minio'] = {
                'status': 'HEALTHY',
                'buckets': [bucket.name for bucket in buckets]
            }
        except Exception as e:
            health_status['components']['minio'] = {
                'status': 'UNHEALTHY',
                'error': str(e)
            }
            health_status['overall_status'] = 'UNHEALTHY'
        
        # Check Analytics API
        try:
            import requests
            response = requests.get('http://localhost:5000/api/health', timeout=5)
            if response.status_code == 200:
                health_status['components']['analytics_api'] = {
                    'status': 'HEALTHY',
                    'response_time_ms': response.elapsed.total_seconds() * 1000
                }
            else:
                health_status['components']['analytics_api'] = {
                    'status': 'UNHEALTHY',
                    'status_code': response.status_code
                }
                health_status['overall_status'] = 'DEGRADED'
        except Exception as e:
            health_status['components']['analytics_api'] = {
                'status': 'UNHEALTHY',
                'error': str(e)
            }
            health_status['overall_status'] = 'DEGRADED'
        
        # Check disk space
        try:
            import shutil
            disk_usage = shutil.disk_usage('.')
            free_space_gb = disk_usage.free / (1024**3)
            
            if free_space_gb > 1:
                health_status['components']['disk_space'] = {
                    'status': 'HEALTHY',
                    'free_space_gb': round(free_space_gb, 2)
                }
            else:
                health_status['components']['disk_space'] = {
                    'status': 'WARNING',
                    'free_space_gb': round(free_space_gb, 2)
                }
                health_status['overall_status'] = 'DEGRADED'
        except Exception as e:
            health_status['components']['disk_space'] = {
                'status': 'UNKNOWN',
                'error': str(e)
            }
        
        logger.info(f"🏥 Health check completed: {health_status['overall_status']}")
        
        return health_status
    
    def cleanup_old_data(self) -> Dict:
        """
        Cleanup data lama berdasarkan retention policy
        """
        logger.info("🧹 Starting data cleanup based on retention policy...")
        
        cleanup_report = {
            'timestamp': datetime.now().isoformat(),
            'actions': [],
            'space_freed_mb': 0
        }
        
        try:
            # Cleanup old log files
            log_cutoff = datetime.now() - timedelta(days=self.config['retention']['log_files_days'])
            
            log_files = Path('../logs').glob('*.log')
            for log_file in log_files:
                if datetime.fromtimestamp(log_file.stat().st_mtime) < log_cutoff:
                    size_mb = log_file.stat().st_size / (1024**2)
                    log_file.unlink()
                    cleanup_report['actions'].append(f"Deleted old log: {log_file.name}")
                    cleanup_report['space_freed_mb'] += size_mb
            
            logger.info(f"✅ Cleanup completed. Freed {cleanup_report['space_freed_mb']:.2f}MB")
            
        except Exception as e:
            logger.error(f"❌ Cleanup failed: {e}")
            cleanup_report['error'] = str(e)
        
        return cleanup_report
    
    def _save_job_history(self):
        """Save job history to file"""
        try:
            with open('../logs/job_history.json', 'w') as f:
                json.dump(self.job_history[-100:], f, indent=2)  # Keep last 100 jobs
        except Exception as e:
            logger.warning(f"Could not save job history: {e}")
    
    def _save_quality_report(self, report: Dict):
        """Save quality report"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            with open(f'../logs/quality_report_{timestamp}.json', 'w') as f:
                json.dump(report, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save quality report: {e}")
    
    def setup_schedules(self):
        """Setup automated schedules"""
        logger.info("⏰ Setting up automated schedules...")
        
        # Daily full ETL
        schedule.every().day.at(self.config['schedules']['full_etl']).do(
            lambda: self.run_etl_pipeline('full')
        )
        
        # Data quality checks
        schedule.every().day.at(self.config['schedules']['data_quality_check']).do(
            self.run_data_quality_checks
        )
        
        # Health checks every 5 minutes
        schedule.every(5).minutes.do(self.run_health_check)
        
        # Daily cleanup
        schedule.every().day.at(self.config['schedules']['cleanup']).do(
            self.cleanup_old_data
        )
        
        logger.info("✅ Schedules configured successfully")
    
    def start_orchestrator(self):
        """Start the orchestrator with continuous monitoring"""
        logger.info("🎯 Starting Data Lakehouse Orchestrator...")
        
        self.setup_schedules()
        
        # Run initial health check
        self.run_health_check()
        
        logger.info("🔄 Orchestrator is running. Press Ctrl+C to stop.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("🛑 Orchestrator stopped by user")
        except Exception as e:
            logger.error(f"❌ Orchestrator error: {e}")
    
    def get_status_dashboard(self) -> Dict:
        """Get current status untuk monitoring dashboard"""
        return {
            'orchestrator_status': 'RUNNING',
            'last_etl_job': self.job_history[-1] if self.job_history else None,
            'pending_jobs': len(schedule.jobs),
            'metrics': self.metrics,
            'timestamp': datetime.now().isoformat()
        }

def main():
    """Main function untuk menjalankan orchestrator"""
    orchestrator = DataLakehouseOrchestrator()
    
    import sys
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == 'run-etl':
            pipeline_type = sys.argv[2] if len(sys.argv) > 2 else 'full'
            result = orchestrator.run_etl_pipeline(pipeline_type)
            print(json.dumps(result, indent=2))
        
        elif command == 'quality-check':
            result = orchestrator.run_data_quality_checks()
            print(json.dumps(result, indent=2))
        
        elif command == 'health-check':
            result = orchestrator.run_health_check()
            print(json.dumps(result, indent=2))
        
        elif command == 'cleanup':
            result = orchestrator.cleanup_old_data()
            print(json.dumps(result, indent=2))
        
        elif command == 'status':
            result = orchestrator.get_status_dashboard()
            print(json.dumps(result, indent=2))
        
        else:
            print("Available commands: run-etl, quality-check, health-check, cleanup, status")
    
    else:
        # Start continuous orchestrator
        orchestrator.start_orchestrator()

if __name__ == "__main__":
    main()