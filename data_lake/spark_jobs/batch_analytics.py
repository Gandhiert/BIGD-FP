#!/usr/bin/env python3
"""
Advanced Gaming Analytics dengan Apache Spark (Batch Processing)
Processes large-scale gaming data untuk insights dan machine learning
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AdvancedGamingAnalytics:
    """
    Advanced analytics engine untuk gaming data dengan Spark ML
    """
    
    def __init__(self,
                 minio_endpoint="localhost:9000",
                 minio_access_key="minioadmin",
                 minio_secret_key="minioadmin"):
        """
        Initialize Spark session dengan ML libraries
        """
        self.minio_endpoint = minio_endpoint
        
        # Initialize Spark session dengan comprehensive config
        self.spark = SparkSession.builder \
            .appName("AdvancedGamingAnalytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session initialized for advanced analytics")
    
    def load_gaming_data(self):
        """Load dan combine semua gaming data"""
        try:
            # Load batch data dari warehouse
            games_df = self.spark.read.parquet("s3a://warehouse-zone/games.parquet")
            reviews_df = self.spark.read.parquet("s3a://warehouse-zone/reviews.parquet")
            logs_df = self.spark.read.parquet("s3a://warehouse-zone/logs.parquet")
            
            # Load streaming data jika ada
            try:
                streaming_reviews = self.spark.read.parquet("s3a://streaming-zone/streaming/game_reviews/")
                streaming_events = self.spark.read.parquet("s3a://streaming-zone/streaming/player_events/")
                logger.info("✅ Loaded streaming data")
            except:
                logger.info("⚠️ No streaming data found, using batch data only")
                streaming_reviews = None
                streaming_events = None
            
            return {
                'games': games_df,
                'reviews': reviews_df,
                'logs': logs_df,
                'streaming_reviews': streaming_reviews,
                'streaming_events': streaming_events
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to load gaming data: {e}")
            return None
    
    def player_segmentation_analysis(self, data):
        """Advanced player segmentation menggunakan K-Means clustering"""
        logger.info("🔍 Performing player segmentation analysis...")
        
        reviews_df = data['reviews']
        
        # Aggregate player features
        player_features = reviews_df.groupBy("review_id") \
            .agg(
                avg("playtime_hours").alias("avg_playtime"),
                count("*").alias("total_reviews"),
                avg("helpful_votes").alias("avg_helpful_votes"),
                avg("total_votes").alias("avg_total_votes"),
                sum(when(col("helpful_votes") > col("total_votes") / 2, 1).otherwise(0)).alias("positive_reviews"),
                stddev("playtime_hours").alias("playtime_variance")
            ).fillna(0)
        
        # Feature engineering
        player_features = player_features.withColumn(
            "review_helpfulness_ratio",
            when(col("avg_total_votes") > 0, col("avg_helpful_votes") / col("avg_total_votes")).otherwise(0)
        ).withColumn(
            "positive_review_ratio",
            when(col("total_reviews") > 0, col("positive_reviews") / col("total_reviews")).otherwise(0)
        )
        
        # Prepare features untuk clustering
        feature_cols = ["avg_playtime", "total_reviews", "review_helpfulness_ratio", "positive_review_ratio"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # K-Means clustering
        kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=5, seed=42)
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Train model
        model = pipeline.fit(player_features)
        clustered_players = model.transform(player_features)
        
        # Analyze clusters
        cluster_analysis = clustered_players.groupBy("cluster") \
            .agg(
                count("*").alias("player_count"),
                avg("avg_playtime").alias("avg_playtime"),
                avg("total_reviews").alias("avg_reviews"),
                avg("review_helpfulness_ratio").alias("avg_helpfulness"),
                avg("positive_review_ratio").alias("avg_positivity")
            ) \
            .orderBy("cluster")
        
        # Show results
        logger.info("📊 Player Segmentation Results:")
        cluster_analysis.show(20, False)
        
        # Save results
        clustered_players.write.mode("overwrite").parquet("s3a://warehouse-zone/analytics/player_segments.parquet")
        cluster_analysis.write.mode("overwrite").parquet("s3a://warehouse-zone/analytics/cluster_analysis.parquet")
        
        return clustered_players, cluster_analysis
    
    def game_performance_prediction(self, data):
        """Predict game performance berdasarkan features"""
        logger.info("🎯 Building game performance prediction model...")
        
        reviews_df = data['reviews']
        
        # Aggregate game features
        game_features = reviews_df.groupBy("review_id") \
            .agg(
                count("*").alias("total_reviews"),
                avg("playtime_hours").alias("avg_playtime"),
                avg("helpful_votes").alias("avg_helpful_votes"),
                stddev("playtime_hours").alias("playtime_variance"),
                sum(when(col("helpful_votes") > col("total_votes") / 2, 1).otherwise(0)).alias("positive_reviews")
            ).fillna(0)
        
        # Target variable: game success score
        game_features = game_features.withColumn(
            "success_score",
            (col("positive_reviews") / col("total_reviews") * 100 + 
             least(col("avg_playtime") / 10, lit(100))) / 2
        )
        
        # Feature engineering
        feature_cols = ["total_reviews", "avg_playtime", "avg_helpful_votes", "playtime_variance"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        # Train-test split
        train_data, test_data = game_features.randomSplit([0.8, 0.2], seed=42)
        
        # Linear regression model
        lr = LinearRegression(featuresCol="features", labelCol="success_score")
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, lr])
        
        # Train model
        model = pipeline.fit(train_data)
        
        # Make predictions
        predictions = model.transform(test_data)
        
        # Evaluate model
        evaluator = RegressionEvaluator(labelCol="success_score", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        
        logger.info(f"📈 Game Performance Prediction RMSE: {rmse:.2f}")
        
        # Save model dan predictions
        predictions.write.mode("overwrite").parquet("s3a://warehouse-zone/analytics/game_performance_predictions.parquet")
        
        return model, predictions, rmse
    
    def sentiment_trend_analysis(self, data):
        """Advanced sentiment trend analysis over time"""
        logger.info("📈 Analyzing sentiment trends over time...")
        
        reviews_df = data['reviews']
        
        # Convert review_id to timestamp (simulation)
        # In real scenario, you'd have actual timestamp columns
        reviews_with_time = reviews_df.withColumn(
            "review_timestamp",
            from_unixtime(unix_timestamp() - (hash(col("review_id")) % 86400) * 30)
        )
        
        # Calculate sentiment score
        sentiment_scores = reviews_with_time.withColumn(
            "sentiment_score",
            when(col("helpful_votes") > col("total_votes") / 2, 1)
            .when(col("helpful_votes") < col("total_votes") / 4, -1)
            .otherwise(0)
        )
        
        # Time-based aggregations
        daily_sentiment = sentiment_scores \
            .withColumn("date", to_date(col("review_timestamp"))) \
            .groupBy("date") \
            .agg(
                avg("sentiment_score").alias("avg_sentiment"),
                count("*").alias("review_count"),
                sum(when(col("sentiment_score") > 0, 1).otherwise(0)).alias("positive_count"),
                sum(when(col("sentiment_score") < 0, 1).otherwise(0)).alias("negative_count")
            ) \
            .orderBy("date")
        
        # Weekly trends
        weekly_sentiment = sentiment_scores \
            .withColumn("week", date_trunc("week", col("review_timestamp"))) \
            .groupBy("week") \
            .agg(
                avg("sentiment_score").alias("avg_sentiment"),
                count("*").alias("review_count"),
                variance("sentiment_score").alias("sentiment_variance")
            ) \
            .orderBy("week")
        
        logger.info("📊 Daily Sentiment Trends:")
        daily_sentiment.show(20, False)
        
        # Save results
        daily_sentiment.write.mode("overwrite").parquet("s3a://warehouse-zone/analytics/daily_sentiment_trends.parquet")
        weekly_sentiment.write.mode("overwrite").parquet("s3a://warehouse-zone/analytics/weekly_sentiment_trends.parquet")
        
        return daily_sentiment, weekly_sentiment
    
    def server_performance_analysis(self, data):
        """Analyze server performance patterns"""
        logger.info("🖥️ Analyzing server performance patterns...")
        
        logs_df = data['logs']
        
        # Server performance metrics
        server_metrics = logs_df.groupBy("server_id") \
            .agg(
                count("*").alias("total_events"),
                countDistinct("player_id").alias("unique_players"),
                sum(when(col("level") == "ERROR", 1).otherwise(0)).alias("error_count"),
                sum(when(col("level") == "WARNING", 1).otherwise(0)).alias("warning_count"),
                avg("response_time_ms").alias("avg_response_time"),
                max("response_time_ms").alias("max_response_time")
            )
        
        # Calculate performance score
        server_metrics = server_metrics.withColumn(
            "performance_score",
            100 - (col("error_count") / col("total_events") * 100) - 
            (col("avg_response_time") / 1000 * 10)
        )
        
        # Identify problematic servers
        problematic_servers = server_metrics.filter(
            (col("performance_score") < 70) | 
            (col("error_count") > col("total_events") * 0.05)
        )
        
        logger.info("⚠️ Problematic Servers:")
        problematic_servers.show(20, False)
        
        # Regional performance analysis
        regional_performance = logs_df.groupBy("region") \
            .agg(
                count("*").alias("total_events"),
                countDistinct("server_id").alias("server_count"),
                avg("response_time_ms").alias("avg_response_time"),
                sum(when(col("level") == "ERROR", 1).otherwise(0)).alias("total_errors")
            )
        
        logger.info("🌍 Regional Performance:")
        regional_performance.show(20, False)
        
        # Save results
        server_metrics.write.mode("overwrite").parquet("s3a://warehouse-zone/analytics/server_performance_metrics.parquet")
        regional_performance.write.mode("overwrite").parquet("s3a://warehouse-zone/analytics/regional_performance.parquet")
        
        return server_metrics, regional_performance
    
    def anomaly_detection(self, data):
        """Detect anomalies dalam gaming data"""
        logger.info("🚨 Running anomaly detection...")
        
        reviews_df = data['reviews']
        
        # Player behavior anomalies
        player_stats = reviews_df.groupBy("review_id") \
            .agg(
                count("*").alias("review_count"),
                avg("playtime_hours").alias("avg_playtime"),
                stddev("playtime_hours").alias("playtime_stddev"),
                max("playtime_hours").alias("max_playtime")
            ).fillna(0)
        
        # Calculate z-scores untuk anomaly detection
        playtime_mean = player_stats.agg(avg("avg_playtime")).collect()[0][0]
        playtime_std = player_stats.agg(stddev("avg_playtime")).collect()[0][0]
        
        anomalies = player_stats.withColumn(
            "playtime_zscore",
            abs((col("avg_playtime") - lit(playtime_mean)) / lit(playtime_std))
        ).filter(
            (col("playtime_zscore") > 3) |  # Statistical outliers
            (col("max_playtime") > 10000) |  # Extremely high playtime
            (col("review_count") > 100)  # Unusual review activity
        )
        
        logger.info(f"🚨 Found {anomalies.count()} anomalous players")
        anomalies.show(20, False)
        
        # Save anomalies
        anomalies.write.mode("overwrite").parquet("s3a://warehouse-zone/analytics/player_anomalies.parquet")
        
        return anomalies
    
    def generate_comprehensive_report(self, data):
        """Generate comprehensive analytics report"""
        logger.info("📋 Generating comprehensive analytics report...")
        
        reviews_df = data['reviews']
        logs_df = data['logs']
        
        # Overall statistics
        total_reviews = reviews_df.count()
        total_logs = logs_df.count()
        unique_players = reviews_df.select("review_id").distinct().count()
        
        # Gaming trends
        avg_playtime = reviews_df.agg(avg("playtime_hours")).collect()[0][0]
        avg_helpful_ratio = reviews_df.agg(
            avg(col("helpful_votes") / (col("total_votes") + 1))
        ).collect()[0][0]
        
        # Server health
        error_rate = logs_df.filter(col("level") == "ERROR").count() / total_logs * 100
        avg_response_time = logs_df.agg(avg("response_time_ms")).collect()[0][0]
        
        # Create report DataFrame
        report_data = [
            ("total_reviews", total_reviews),
            ("total_logs", total_logs),
            ("unique_players", unique_players),
            ("avg_playtime_hours", round(avg_playtime, 2)),
            ("avg_helpful_ratio", round(avg_helpful_ratio, 3)),
            ("error_rate_percent", round(error_rate, 2)),
            ("avg_response_time_ms", round(avg_response_time, 2))
        ]
        
        report_df = self.spark.createDataFrame(report_data, ["metric", "value"])
        
        logger.info("📊 Comprehensive Analytics Report:")
        report_df.show(20, False)
        
        # Save report
        report_df.write.mode("overwrite").parquet("s3a://warehouse-zone/analytics/comprehensive_report.parquet")
        
        return report_df
    
    def run_full_analytics_pipeline(self):
        """Run complete analytics pipeline"""
        logger.info("🚀 Starting full analytics pipeline...")
        
        # Load data
        data = self.load_gaming_data()
        if not data:
            logger.error("❌ Failed to load data")
            return
        
        results = {}
        
        try:
            # 1. Player Segmentation
            results['segmentation'] = self.player_segmentation_analysis(data)
            
            # 2. Game Performance Prediction
            results['prediction'] = self.game_performance_prediction(data)
            
            # 3. Sentiment Trend Analysis
            results['sentiment'] = self.sentiment_trend_analysis(data)
            
            # 4. Server Performance Analysis
            results['server_performance'] = self.server_performance_analysis(data)
            
            # 5. Anomaly Detection
            results['anomalies'] = self.anomaly_detection(data)
            
            # 6. Comprehensive Report
            results['report'] = self.generate_comprehensive_report(data)
            
            logger.info("✅ Full analytics pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Analytics pipeline error: {e}")
        
        finally:
            self.spark.stop()
        
        return results

def main():
    """Main function untuk menjalankan batch analytics"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Advanced Gaming Analytics with Spark')
    parser.add_argument('--minio-endpoint', default='localhost:9000', help='MinIO endpoint')
    parser.add_argument('--analysis-type', 
                       choices=['full', 'segmentation', 'prediction', 'sentiment', 'server', 'anomaly'],
                       default='full', help='Type of analysis to run')
    
    args = parser.parse_args()
    
    # Create analytics engine
    analytics = AdvancedGamingAnalytics(minio_endpoint=args.minio_endpoint)
    
    if args.analysis_type == 'full':
        analytics.run_full_analytics_pipeline()
    else:
        data = analytics.load_gaming_data()
        if data:
            if args.analysis_type == 'segmentation':
                analytics.player_segmentation_analysis(data)
            elif args.analysis_type == 'prediction':
                analytics.game_performance_prediction(data)
            elif args.analysis_type == 'sentiment':
                analytics.sentiment_trend_analysis(data)
            elif args.analysis_type == 'server':
                analytics.server_performance_analysis(data)
            elif args.analysis_type == 'anomaly':
                analytics.anomaly_detection(data)

if __name__ == "__main__":
    main() 