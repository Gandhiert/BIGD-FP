#!/usr/bin/env python3
"""
PySpark MLlib Games Clustering Job
Real-time clustering untuk games data dengan K-Means (k=13)
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline

# MinIO integration
from minio import Minio
from io import BytesIO
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GamesClusteringJob:
    """
    PySpark job untuk real-time clustering games data
    """
    
    def __init__(self,
                 app_name="GamesClusteringJob",
                 minio_endpoint="localhost:9000",
                 minio_access_key="minioadmin",
                 minio_secret_key="minioadmin",
                 input_bucket="streaming-zone",
                 output_bucket="clusters-zone",
                 k_clusters=13):
        """
        Initialize Games Clustering Job
        
        Args:
            app_name: Nama Spark application
            minio_endpoint: MinIO endpoint
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
            input_bucket: Bucket untuk input data
            output_bucket: Bucket untuk output clusters
            k_clusters: Jumlah clusters (default: 13)
        """
        self.app_name = app_name
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.k_clusters = k_clusters
        
        # Initialize MinIO client
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        # Spark session
        self.spark = None
        
        # Feature columns untuk clustering
        self.feature_columns = [
            'positive_ratio',
            'user_reviews_log',
            'price_final',
            'discount',
            'rating_encoded',
            'platform_count',
            'price_category',
            'steam_deck_numeric',
            'release_year'
        ]
        
        logger.info(f"🎯 Initialized Games Clustering Job")
        logger.info(f"📊 Target clusters: {k_clusters}")
        logger.info(f"🗄️ Input bucket: {input_bucket}")
        logger.info(f"📦 Output bucket: {output_bucket}")
    
    def create_spark_session(self) -> SparkSession:
        """Create Spark session dengan konfigurasi optimal"""
        try:
            logger.info("🚀 Creating Spark session...")
            
            # Configure Spark untuk MinIO integration
            spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
                .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}") \
                .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()
            
            # Set log level
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info("✅ Spark session created successfully")
            logger.info(f"🎯 Spark version: {spark.version}")
            logger.info(f"💻 Available cores: {spark.sparkContext.defaultParallelism}")
            
            return spark
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            raise
    
    def ensure_buckets_exist(self):
        """Ensure MinIO buckets exist"""
        try:
            buckets_to_check = [self.input_bucket, self.output_bucket]
            
            for bucket in buckets_to_check:
                if not self.minio_client.bucket_exists(bucket):
                    self.minio_client.make_bucket(bucket)
                    logger.info(f"✅ Created bucket: {bucket}")
                else:
                    logger.info(f"📦 Bucket exists: {bucket}")
                    
        except Exception as e:
            logger.error(f"❌ Error ensuring buckets exist: {e}")
            raise
    
    def load_streaming_games_data(self, hours_back=24) -> Optional[DataFrame]:
        """
        Load games data dari streaming bucket
        
        Args:
            hours_back: Hours of data to load
            
        Returns:
            Spark DataFrame dengan games data
        """
        try:
            logger.info(f"📂 Loading streaming games data from last {hours_back} hours...")
            
            # List objects dalam streaming bucket
            prefix = "streaming/games_stream/"
            objects = list(self.minio_client.list_objects(
                self.input_bucket, 
                prefix=prefix, 
                recursive=True
            ))
            
            if not objects:
                logger.warning("⚠️ No streaming data found")
                return None
            
            # Filter recent objects
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            recent_files = []
            
            for obj in objects:
                try:
                    # Extract timestamp dari filename
                    filename = obj.object_name.split('/')[-1]
                    if filename.startswith('batch_') and filename.endswith('.parquet'):
                        timestamp_str = filename.replace('batch_', '').replace('.parquet', '')
                        file_time = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                        
                        if file_time >= cutoff_time:
                            recent_files.append(f"s3a://{self.input_bucket}/{obj.object_name}")
                except ValueError:
                    continue
            
            if not recent_files:
                logger.warning("⚠️ No recent streaming files found")
                return None
            
            logger.info(f"📊 Found {len(recent_files)} recent streaming files")
            
            # Read files into Spark DataFrame
            if len(recent_files) == 1:
                df = self.spark.read.parquet(recent_files[0])
            else:
                df = self.spark.read.parquet(*recent_files)
            
            logger.info(f"✅ Loaded {df.count():,} games records")
            logger.info(f"📋 Schema: {df.columns}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error loading streaming data: {e}")
            return None
    
    def load_games_csv_direct(self, csv_path="../../games.csv") -> Optional[DataFrame]:
        """
        Load games.csv directly (fallback method)
        
        Args:
            csv_path: Path to games.csv
            
        Returns:
            Spark DataFrame
        """
        try:
            logger.info(f"📂 Loading games data directly from CSV: {csv_path}")
            
            # Define schema
            schema = StructType([
                StructField("app_id", IntegerType(), True),
                StructField("title", StringType(), True),
                StructField("date_release", StringType(), True),
                StructField("win", BooleanType(), True),
                StructField("mac", BooleanType(), True),
                StructField("linux", BooleanType(), True),
                StructField("rating", StringType(), True),
                StructField("positive_ratio", IntegerType(), True),
                StructField("user_reviews", IntegerType(), True),
                StructField("price_final", DoubleType(), True),
                StructField("price_original", DoubleType(), True),
                StructField("discount", DoubleType(), True),
                StructField("steam_deck", BooleanType(), True)
            ])
            
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(schema) \
                .csv(csv_path)
            
            logger.info(f"✅ Loaded {df.count():,} games from CSV")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error loading CSV: {e}")
            return None
    
    def preprocess_features(self, df: DataFrame) -> DataFrame:
        """
        Preprocess features untuk clustering
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame dengan features yang sudah diproses
        """
        try:
            logger.info("⚙️ Preprocessing features untuk clustering...")
            
            # Add derived features
            df_processed = df \
                .withColumn("platform_count", 
                           col("win").cast("int") + 
                           col("mac").cast("int") + 
                           col("linux").cast("int")) \
                .withColumn("steam_deck_numeric", col("steam_deck").cast("int")) \
                .withColumn("user_reviews_log", 
                           when(col("user_reviews") > 0, 
                                log10(col("user_reviews") + 1)).otherwise(0)) \
                .withColumn("release_year", 
                           year(to_date(col("date_release"), "yyyy-MM-dd"))) \
                .withColumn("price_category",
                           when(col("price_final") == 0, 0)
                           .when(col("price_final") <= 5, 1)
                           .when(col("price_final") <= 20, 2)
                           .when(col("price_final") <= 50, 3)
                           .otherwise(4))
            
            # Encode rating
            rating_indexer = StringIndexer(
                inputCol="rating",
                outputCol="rating_encoded",
                handleInvalid="keep"
            )
            
            df_processed = rating_indexer.fit(df_processed).transform(df_processed)
            
            # Filter valid records
            df_processed = df_processed.filter(
                col("positive_ratio").isNotNull() &
                col("user_reviews").isNotNull() &
                col("price_final").isNotNull() &
                (col("positive_ratio") >= 0) &
                (col("positive_ratio") <= 100)
            )
            
            # Fill missing values
            df_processed = df_processed.fillna({
                "discount": 0.0,
                "release_year": 2020  # Default year
            })
            
            logger.info(f"✅ Preprocessing completed. Records: {df_processed.count():,}")
            logger.info(f"📊 Feature columns: {self.feature_columns}")
            
            return df_processed
            
        except Exception as e:
            logger.error(f"❌ Error preprocessing features: {e}")
            raise
    
    def create_clustering_pipeline(self) -> Pipeline:
        """
        Create ML pipeline untuk clustering
        
        Returns:
            Configured ML Pipeline
        """
        try:
            logger.info("🔧 Creating clustering pipeline...")
            
            # Vector assembler
            vector_assembler = VectorAssembler(
                inputCols=self.feature_columns,
                outputCol="features_raw",
                handleInvalid="skip"
            )
            
            # Standard scaler
            scaler = StandardScaler(
                inputCol="features_raw",
                outputCol="features",
                withStd=True,
                withMean=True
            )
            
            # K-Means clustering
            kmeans = KMeans(
                featuresCol="features",
                predictionCol="cluster",
                k=self.k_clusters,
                seed=42,
                maxIter=100,
                tol=1e-4,
                initMode="k-means||",
                initSteps=5
            )
            
            # Create pipeline
            pipeline = Pipeline(stages=[vector_assembler, scaler, kmeans])
            
            logger.info(f"✅ Pipeline created with {self.k_clusters} clusters")
            return pipeline
            
        except Exception as e:
            logger.error(f"❌ Error creating pipeline: {e}")
            raise
    
    def evaluate_clustering(self, df_clustered: DataFrame) -> Dict:
        """
        Evaluate clustering quality
        
        Args:
            df_clustered: DataFrame dengan cluster assignments
            
        Returns:
            Dictionary dengan metrics
        """
        try:
            logger.info("📊 Evaluating clustering quality...")
            
            # Silhouette evaluator
            evaluator = ClusteringEvaluator(
                featuresCol="features",
                predictionCol="cluster",
                metricName="silhouette",
                distanceMeasure="squaredEuclidean"
            )
            
            silhouette_score = evaluator.evaluate(df_clustered)
            
            # Cluster distribution
            cluster_counts = df_clustered.groupBy("cluster").count().collect()
            cluster_distribution = {int(row['cluster']): int(row['count']) for row in cluster_counts}
            
            # Calculate cluster statistics
            total_records = sum(list(cluster_distribution.values()))
            cluster_percentages = {
                cluster: (count / total_records) * 100 
                for cluster, count in cluster_distribution.items()
            }
            
            metrics = {
                'silhouette_score': float(silhouette_score),
                'total_records': int(total_records),
                'num_clusters': len(cluster_distribution),
                'cluster_distribution': cluster_distribution,
                'cluster_percentages': cluster_percentages,
                'evaluation_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"📈 Silhouette Score: {silhouette_score:.4f}")
            logger.info(f"📊 Cluster distribution: {cluster_distribution}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Error evaluating clustering: {e}")
            return {}
    
    def save_clusters_to_minio(self, df_clustered: DataFrame, metrics: Dict) -> bool:
        """
        Save clustering results ke MinIO
        
        Args:
            df_clustered: DataFrame dengan cluster assignments
            metrics: Clustering metrics
            
        Returns:
            Success status
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save clustered data
            logger.info("💾 Saving clustered data to MinIO...")
            
            # Convert to Pandas untuk easier handling - pilih kolom unique saja
            base_columns = ["app_id", "title", "rating", "user_reviews", "cluster"]
            
            # Tambahkan feature columns yang belum ada di base_columns
            additional_columns = []
            for col in self.feature_columns:
                if col not in base_columns:
                    additional_columns.append(col)
            
            all_columns = base_columns + additional_columns
            df_pandas = df_clustered.select(*all_columns).toPandas()
            
            # Save main clusters file
            clusters_filename = f"clusters/games_clusters_{timestamp}.parquet"
            clusters_buffer = BytesIO()
            df_pandas.to_parquet(clusters_buffer, index=False)
            clusters_buffer.seek(0)
            
            self.minio_client.put_object(
                self.output_bucket,
                clusters_filename,
                clusters_buffer,
                length=clusters_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            # Save cluster analysis
            analysis_data = []
            for cluster_id in range(self.k_clusters):
                cluster_data = df_pandas[df_pandas['cluster'] == cluster_id]
                if len(cluster_data) > 0:
                    analysis = {
                        'cluster_id': int(cluster_id),
                        'game_count': len(cluster_data),
                        'avg_user_reviews': float(cluster_data['user_reviews'].mean()) if 'user_reviews' in cluster_data.columns else 0.0,
                    }
                    
                    # Add available feature columns to analysis
                    for col in self.feature_columns:
                        if col in cluster_data.columns:
                            analysis[f'avg_{col}'] = float(cluster_data[col].mean())
                    
                    # Add sample games (menggunakan kolom yang tersedia)
                    sample_columns = ['app_id', 'title']
                    if 'price_final' in cluster_data.columns:
                        sample_columns.append('price_final')
                    
                    analysis['sample_games'] = cluster_data[sample_columns].head(5).to_dict('records')
                    analysis_data.append(analysis)
            
            # Save cluster analysis
            analysis_filename = f"analysis/cluster_analysis_{timestamp}.json"
            analysis_with_metrics = {
                'timestamp': timestamp,
                'clustering_metrics': metrics,
                'cluster_analysis': analysis_data
            }
            
            analysis_buffer = BytesIO(json.dumps(analysis_with_metrics, indent=2).encode('utf-8'))
            self.minio_client.put_object(
                self.output_bucket,
                analysis_filename,
                analysis_buffer,
                length=analysis_buffer.getbuffer().nbytes,
                content_type='application/json'
            )
            
            # Save latest clusters (untuk API)
            latest_clusters_filename = "latest/games_clusters_latest.parquet"
            clusters_buffer.seek(0)
            self.minio_client.put_object(
                self.output_bucket,
                latest_clusters_filename,
                clusters_buffer,
                length=clusters_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            # Save latest analysis (untuk API)
            latest_analysis_filename = "latest/cluster_analysis_latest.json"
            analysis_buffer.seek(0)
            self.minio_client.put_object(
                self.output_bucket,
                latest_analysis_filename,
                analysis_buffer,
                length=analysis_buffer.getbuffer().nbytes,
                content_type='application/json'
            )
            
            logger.info(f"✅ Clustering results saved successfully")
            logger.info(f"📂 Clusters file: {clusters_filename}")
            logger.info(f"📂 Latest clusters file: {latest_clusters_filename}")
            logger.info(f"📊 Analysis file: {analysis_filename}")
            logger.info(f"📊 Latest analysis file: {latest_analysis_filename}")
            
            # Verify files were saved by checking their existence
            try:
                # Check latest files exist
                latest_clusters_obj = self.minio_client.stat_object(self.output_bucket, latest_clusters_filename)
                latest_analysis_obj = self.minio_client.stat_object(self.output_bucket, latest_analysis_filename)
                
                logger.info(f"✅ Verification - Latest clusters file exists: size={latest_clusters_obj.size}, modified={latest_clusters_obj.last_modified}")
                logger.info(f"✅ Verification - Latest analysis file exists: size={latest_analysis_obj.size}, modified={latest_analysis_obj.last_modified}")
                
            except Exception as verify_error:
                logger.warning(f"⚠️ Could not verify saved files: {verify_error}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving to MinIO: {e}")
            return False
    
    def run_clustering_job(self, use_streaming_data=True, hours_back=24) -> bool:
        """
        Run complete clustering job
        
        Args:
            use_streaming_data: Whether to use streaming data or CSV
            hours_back: Hours of streaming data to use
            
        Returns:
            Success status
        """
        try:
            logger.info("🚀 Starting Games Clustering Job")
            logger.info("=" * 60)
            
            # Create Spark session
            self.spark = self.create_spark_session()
            
            # Ensure buckets exist
            self.ensure_buckets_exist()
            
            # Load data
            if use_streaming_data:
                df = self.load_streaming_games_data(hours_back=hours_back)
                if df is None:
                    logger.info("⚠️ No streaming data available, falling back to CSV")
                    df = self.load_games_csv_direct()
            else:
                df = self.load_games_csv_direct()
            
            if df is None:
                logger.error("❌ No data available for clustering")
                return False
            
            # Preprocess features
            df_processed = self.preprocess_features(df)
            
            # Create and fit pipeline
            pipeline = self.create_clustering_pipeline()
            logger.info("🎯 Training clustering model...")
            
            model = pipeline.fit(df_processed)
            
            # Apply clustering
            logger.info("🔮 Applying clustering to data...")
            df_clustered = model.transform(df_processed)
            
            # Evaluate clustering
            metrics = self.evaluate_clustering(df_clustered)
            
            # Save results
            success = self.save_clusters_to_minio(df_clustered, metrics)
            
            if success:
                logger.info("🎉 Clustering job completed successfully!")
                logger.info(f"📊 Processed {df_clustered.count():,} games")
                logger.info(f"🎯 Created {self.k_clusters} clusters")
                silhouette = metrics.get('silhouette_score', 0.0)
                if isinstance(silhouette, (int, float)):
                    logger.info(f"📈 Silhouette Score: {silhouette:.4f}")
                else:
                    logger.info(f"📈 Silhouette Score: N/A")
            else:
                logger.error("❌ Clustering job failed during save")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Clustering job failed: {e}")
            return False
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("🛑 Spark session stopped")


def main():
    """Main function untuk menjalankan clustering job"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Games Clustering Job')
    parser.add_argument('--minio-endpoint', default='localhost:9000',
                       help='MinIO endpoint')
    parser.add_argument('--minio-access-key', default='minioadmin',
                       help='MinIO access key')
    parser.add_argument('--minio-secret-key', default='minioadmin',
                       help='MinIO secret key')
    parser.add_argument('--input-bucket', default='streaming-zone',
                       help='Input bucket name')
    parser.add_argument('--output-bucket', default='clusters-zone',
                       help='Output bucket name')
    parser.add_argument('--k-clusters', type=int, default=13,
                       help='Number of clusters')
    parser.add_argument('--use-csv', action='store_true',
                       help='Use CSV instead of streaming data')
    parser.add_argument('--hours-back', type=int, default=24,
                       help='Hours of streaming data to process')
    
    args = parser.parse_args()
    
    # Create clustering job
    clustering_job = GamesClusteringJob(
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
        input_bucket=args.input_bucket,
        output_bucket=args.output_bucket,
        k_clusters=args.k_clusters
    )
    
    logger.info("🎮 Starting Games Clustering Job")
    logger.info(f"⚙️ Configuration:")
    logger.info(f"   MinIO: {args.minio_endpoint}")
    logger.info(f"   Input bucket: {args.input_bucket}")
    logger.info(f"   Output bucket: {args.output_bucket}")
    logger.info(f"   K clusters: {args.k_clusters}")
    logger.info(f"   Use streaming: {not args.use_csv}")
    
    try:
        success = clustering_job.run_clustering_job(
            use_streaming_data=not args.use_csv,
            hours_back=args.hours_back
        )
        
        if success:
            logger.info("✅ Clustering job completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Clustering job failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Job failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()