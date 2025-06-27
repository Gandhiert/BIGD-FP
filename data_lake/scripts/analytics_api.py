from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
from minio import Minio
import duckdb
from io import BytesIO
import os
import logging
from datetime import datetime, timedelta

# Import sentiment analyzer
try:
    from sentiment_analyzer import GameReviewSentimentAnalyzer, analyze_review_dataframe
    SENTIMENT_ANALYZER_AVAILABLE = True
except ImportError:
    SENTIMENT_ANALYZER_AVAILABLE = False
    logging.warning("⚠️ Advanced sentiment analyzer not available. Using fallback method.")

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO client
client = Minio(
    'localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

WAREHOUSE_BUCKET = 'warehouse-zone'
STREAMING_BUCKET = 'streaming-zone'
CLUSTERS_BUCKET = 'clusters-zone'

# Global sentiment analyzer instance
sentiment_analyzer = None
# Add caching and lightweight mode
LIGHTWEIGHT_MODE = True  # Set to True untuk mengurangi beban komputasi
sentiment_cache = {}
last_sentiment_analysis = None

if SENTIMENT_ANALYZER_AVAILABLE and not LIGHTWEIGHT_MODE:
    try:
        sentiment_analyzer = GameReviewSentimentAnalyzer(use_transformers=False)  # Set to True for more accuracy
        logger.info("✅ Advanced sentiment analyzer initialized")
    except Exception as e:
        logger.warning(f"⚠️ Could not initialize sentiment analyzer: {e}")
        SENTIMENT_ANALYZER_AVAILABLE = False
elif LIGHTWEIGHT_MODE:
    logger.info("🚀 Running in LIGHTWEIGHT MODE - NLP processing disabled for better performance")
    SENTIMENT_ANALYZER_AVAILABLE = False

def get_parquet_from_minio(filename, bucket=WAREHOUSE_BUCKET):
    """Download Parquet file from MinIO and return as DataFrame"""
    try:
        data = client.get_object(bucket, filename)
        df = pd.read_parquet(BytesIO(data.read()))
        return df
    except Exception as e:
        logger.error(f"Error loading {filename} from {bucket}: {e}")
        return None

def get_streaming_data(event_type, hours_back=24):
    """
    Get streaming data from streaming-zone bucket
    
    Args:
        event_type: Type of event (player_events, game_reviews, server_logs, etc.)
        hours_back: How many hours back to fetch data
    
    Returns:
        Combined DataFrame from all matching files
    """
    try:
        # List objects in streaming bucket for the event type
        prefix = f"streaming/{event_type}/"
        objects = client.list_objects(STREAMING_BUCKET, prefix=prefix, recursive=True)
        
        # Filter objects from last N hours
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        recent_objects = []
        
        for obj in objects:
            # Extract timestamp from filename (format: batch_YYYYMMDD_HHMMSS.parquet)
            try:
                filename = obj.object_name.split('/')[-1]  # Get filename only
                if filename.startswith('batch_') and filename.endswith('.parquet'):
                    timestamp_str = filename.replace('batch_', '').replace('.parquet', '')
                    file_time = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                    
                    if file_time >= cutoff_time:
                        recent_objects.append(obj.object_name)
            except ValueError:
                # Skip files with invalid timestamp format
                continue
        
        if not recent_objects:
            logger.info(f"No recent streaming data found for {event_type}")
            return None
        
        # Read and combine all recent files
        dataframes = []
        for obj_name in recent_objects:
            try:
                df = get_parquet_from_minio(obj_name, bucket=STREAMING_BUCKET)
                if df is not None and len(df) > 0:
                    dataframes.append(df)
            except Exception as e:
                logger.warning(f"Could not read streaming file {obj_name}: {e}")
                continue
        
        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            logger.info(f"✅ Loaded {len(combined_df)} records from {len(dataframes)} streaming files for {event_type}")
            return combined_df
        else:
            logger.info(f"No valid streaming data found for {event_type}")
            return None
            
    except Exception as e:
        logger.error(f"Error loading streaming data for {event_type}: {e}")
        return None

def get_combined_data(data_type, prefer_streaming=True, streaming_hours=24):
    """
    Get combined data from both warehouse and streaming sources
    
    Args:
        data_type: Type of data (reviews, logs, etc.)
        prefer_streaming: Whether to prefer streaming data over warehouse
        streaming_hours: Hours of streaming data to include
    
    Returns:
        Combined DataFrame or warehouse DataFrame as fallback
    """
    # Map data types to their corresponding files and streaming event types
    data_mapping = {
        'reviews': {
            'warehouse_file': 'reviews.parquet',
            'streaming_type': 'game_reviews'
        },
        'logs': {
            'warehouse_file': 'logs.parquet',
            'streaming_type': 'server_logs'
        },
        'player_events': {
            'warehouse_file': None,  # No warehouse equivalent
            'streaming_type': 'player_events'
        },
        'player_stats': {
            'warehouse_file': None,
            'streaming_type': 'player_stats'
        },
        'game_metrics': {
            'warehouse_file': None,
            'streaming_type': 'game_metrics'
        }
    }
    
    if data_type not in data_mapping:
        logger.error(f"Unknown data type: {data_type}")
        return None
    
    mapping = data_mapping[data_type]
    
    # Try to get streaming data first if preferred
    streaming_df = None
    if prefer_streaming:
        streaming_df = get_streaming_data(mapping['streaming_type'], hours_back=streaming_hours)
    
    # Get warehouse data as fallback or supplement
    warehouse_df = None
    if mapping['warehouse_file']:
        warehouse_df = get_parquet_from_minio(mapping['warehouse_file'])
    
    # Combine or return best available data
    if streaming_df is not None and warehouse_df is not None:
        # Combine both sources, prioritizing streaming data
        logger.info(f"Combining warehouse ({len(warehouse_df)}) and streaming ({len(streaming_df)}) data for {data_type}")
        return pd.concat([warehouse_df, streaming_df], ignore_index=True)
    elif streaming_df is not None:
        logger.info(f"Using streaming data only for {data_type}: {len(streaming_df)} records")
        return streaming_df
    elif warehouse_df is not None:
        logger.info(f"Using warehouse data only for {data_type}: {len(warehouse_df)} records")
        return warehouse_df
    else:
        logger.warning(f"No data available for {data_type}")
        return None

def calculate_advanced_sentiment(reviews_df):
    """
    Calculate advanced sentiment analysis for reviews with caching and lightweight mode
    
    Returns:
        Dict with comprehensive sentiment metrics
    """
    global sentiment_cache, last_sentiment_analysis
    
    if reviews_df is None or len(reviews_df) == 0:
        return {
            'sentiment_score': 50.0,
            'confidence': 0.0,
            'positive_percentage': 33.33,
            'negative_percentage': 33.33,
            'neutral_percentage': 33.34,
            'total_analyzed': 0,
            'method': 'none'
        }
    
    # Lightweight mode: use basic sentiment only
    if LIGHTWEIGHT_MODE:
        logger.info("🚀 Using lightweight sentiment analysis (basic mode)")
        result = calculate_basic_sentiment(reviews_df)
        result['method'] = 'lightweight_basic'
        return result
    
    # Check cache first untuk menghindari re-computation
    cache_key = f"sentiment_{len(reviews_df)}_{hash(str(reviews_df['helpful_votes'].sum()))}"
    if cache_key in sentiment_cache:
        logger.info("📋 Using cached sentiment analysis results")
        cached_result = sentiment_cache[cache_key].copy()
        cached_result['method'] = 'cached_advanced'
        return cached_result
    
    # Check if we have review text column
    text_column = None
    possible_text_columns = ['review_text', 'review', 'content', 'text', 'comment']
    
    for col in possible_text_columns:
        if col in reviews_df.columns:
            text_column = col
            break
    
    if not text_column and SENTIMENT_ANALYZER_AVAILABLE:
        # Generate synthetic review text from other columns for demonstration
        logger.info("Generating synthetic review text for sentiment analysis...")
        reviews_df = reviews_df.copy()
        reviews_df['synthetic_review'] = reviews_df.apply(lambda row: 
            f"Game review with {row.get('playtime_hours', 0)} hours played. "
            f"Helpful votes: {row.get('helpful_votes', 0)}, Total votes: {row.get('total_votes', 1)}. "
            f"{'Highly recommended!' if row.get('helpful_votes', 0) > row.get('total_votes', 1)/2 else 'Could be better.'}"
            , axis=1)
        text_column = 'synthetic_review'
    
    if SENTIMENT_ANALYZER_AVAILABLE and text_column and sentiment_analyzer:
        try:
            logger.info(f"🔍 Performing advanced sentiment analysis on {len(reviews_df)} reviews...")
            
            # Reduced sample size untuk performance yang lebih baik
            sample_size = min(200, len(reviews_df))  # Kurangi dari 1000 ke 200
            sample_df = reviews_df.head(sample_size)
            
            # Analyze sentiments
            analyzed_df = analyze_review_dataframe(
                sample_df, 
                text_column, 
                use_transformers=False  # Selalu gunakan False untuk performa
            )
            
            # Calculate comprehensive statistics
            sentiment_stats = sentiment_analyzer.get_summary_statistics([
                {
                    'overall_sentiment': sentiment,
                    'confidence': confidence
                }
                for sentiment, confidence in zip(
                    analyzed_df['sentiment'], 
                    analyzed_df['sentiment_confidence']
                )
            ])
            
            # Add advanced metrics
            sentiment_stats.update({
                'method': 'advanced_nlp',
                'sample_size': sample_size,
                'high_confidence_reviews': len(analyzed_df[analyzed_df['sentiment_confidence'] > 0.7]),
                'positive_high_conf': len(analyzed_df[
                    (analyzed_df['sentiment'] == 'positive') & 
                    (analyzed_df['sentiment_confidence'] > 0.7)
                ]),
                'negative_high_conf': len(analyzed_df[
                    (analyzed_df['sentiment'] == 'negative') & 
                    (analyzed_df['sentiment_confidence'] > 0.7)
                ])
            })
            
            # Cache result untuk 5 menit (menghindari re-computation)
            sentiment_cache[cache_key] = sentiment_stats.copy()
            
            # Clean old cache entries (keep only latest 10)
            if len(sentiment_cache) > 10:
                oldest_key = list(sentiment_cache.keys())[0]
                del sentiment_cache[oldest_key]
            
            logger.info(f"✅ Advanced sentiment analysis complete. Score: {sentiment_stats['sentiment_score']:.1f}")
            return sentiment_stats
            
        except Exception as e:
            logger.error(f"❌ Advanced sentiment analysis failed: {e}")
            # Fall back to basic method
            return calculate_basic_sentiment(reviews_df)
    else:
        # Use basic sentiment analysis as fallback
        return calculate_basic_sentiment(reviews_df)

def calculate_basic_sentiment(reviews_df):
    """Fallback basic sentiment calculation"""
    if reviews_df is None or len(reviews_df) == 0:
        return {
            'sentiment_score': 50.0,
            'confidence': 0.0,
            'positive_percentage': 33.33,
            'negative_percentage': 33.33,
            'neutral_percentage': 33.34,
            'total_analyzed': 0,
            'method': 'basic'
        }
    
    # Basic sentiment using helpful votes ratio
    positive_reviews = len(reviews_df[
        reviews_df['helpful_votes'] > reviews_df['total_votes']/2
    ])
    total_reviews = len(reviews_df)
    
    if total_reviews > 0:
        positive_pct = (positive_reviews / total_reviews) * 100
        negative_pct = ((total_reviews - positive_reviews) / total_reviews) * 100 * 0.7  # Assume 70% of non-positive are negative
        neutral_pct = 100 - positive_pct - negative_pct
        
        # Calculate sentiment score (0-100, 50 is neutral)
        sentiment_score = 50 + (positive_pct - negative_pct) / 2
    else:
        positive_pct = negative_pct = neutral_pct = 33.33
        sentiment_score = 50.0
    
    return {
        'sentiment_score': round(sentiment_score, 2),
        'confidence': 0.6,  # Medium confidence for basic method
        'positive_percentage': round(positive_pct, 2),
        'negative_percentage': round(negative_pct, 2),
        'neutral_percentage': round(neutral_pct, 2),
        'total_analyzed': total_reviews,
        'method': 'basic'
    }

@app.route('/api/dashboard/overview', methods=['GET'])
def dashboard_overview():
    """Overview metrics untuk dashboard utama dengan data streaming"""
    try:
        # Load data from both warehouse and streaming sources
        games_df = get_parquet_from_minio('games.parquet')
        reviews_df = get_combined_data('reviews', prefer_streaming=True, streaming_hours=24)
        logs_df = get_combined_data('logs', prefer_streaming=True, streaming_hours=24)
        
        # Get real-time streaming data
        player_events_df = get_combined_data('player_events', prefer_streaming=True, streaming_hours=1)
        game_metrics_df = get_combined_data('game_metrics', prefer_streaming=True, streaming_hours=1)
        
        # Calculate real-time metrics
        real_time_players = 0
        if player_events_df is not None:
            # Count unique active players in last hour
            recent_events = player_events_df[
                pd.to_datetime(player_events_df['processed_at']) >= 
                datetime.now() - timedelta(hours=1)
            ] if 'processed_at' in player_events_df.columns else player_events_df
            real_time_players = len(recent_events['player_id'].unique()) if len(recent_events) > 0 else 0
        
        # Server health from recent metrics
        server_health_status = 'good'
        if game_metrics_df is not None and len(game_metrics_df) > 0:
            recent_metrics = game_metrics_df.tail(10)  # Last 10 metrics
            if 'health_status' in recent_metrics.columns:
                poor_health_count = len(recent_metrics[recent_metrics['health_status'] == 'poor'])
                if poor_health_count > 3:
                    server_health_status = 'poor'
                elif poor_health_count > 1:
                    server_health_status = 'moderate'
        
        overview = {
            'total_games': len(games_df) if games_df is not None else 0,
            'total_reviews': len(reviews_df) if reviews_df is not None else 0,
            'total_log_entries': len(logs_df) if logs_df is not None else 0,
            'avg_playtime': float(reviews_df['playtime_hours'].mean()) if reviews_df is not None and 'playtime_hours' in reviews_df.columns else 0,
            'sentiment_analysis_enabled': SENTIMENT_ANALYZER_AVAILABLE,
            # Real-time metrics from streaming data
            'real_time_active_players': real_time_players,
            'server_health_status': server_health_status,
            'streaming_data_available': {
                'player_events': player_events_df is not None,
                'game_metrics': game_metrics_df is not None,
                'reviews': reviews_df is not None and len(reviews_df) > 0,
                'logs': logs_df is not None and len(logs_df) > 0
            },
            'last_updated': datetime.now().isoformat()
        }
        
        return jsonify(overview)
    except Exception as e:
        logger.error(f"Dashboard overview error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/player-metrics', methods=['GET'])
def player_metrics():
    """Enhanced player metrics dengan streaming data dan advanced sentiment analysis"""
    try:
        # Get combined data from warehouse and streaming
        reviews_df = get_combined_data('reviews', prefer_streaming=True, streaming_hours=24)
        logs_df = get_combined_data('logs', prefer_streaming=True, streaming_hours=24)
        player_events_df = get_combined_data('player_events', prefer_streaming=True, streaming_hours=24)
        player_stats_df = get_combined_data('player_stats', prefer_streaming=True, streaming_hours=24)
        
        # Playtime distribution from reviews
        playtime_ranges = {}
        if reviews_df is not None and 'playtime_hours' in reviews_df.columns:
            playtime_ranges = {
                '0-10h': len(reviews_df[reviews_df['playtime_hours'] <= 10]),
                '10-50h': len(reviews_df[(reviews_df['playtime_hours'] > 10) & (reviews_df['playtime_hours'] <= 50)]),
                '50-100h': len(reviews_df[(reviews_df['playtime_hours'] > 50) & (reviews_df['playtime_hours'] <= 100)]),
                '100h+': len(reviews_df[reviews_df['playtime_hours'] > 100])
            }
        
        # Player categories from streaming player stats
        player_categories = {'casual': 0, 'regular': 0, 'hardcore': 0}
        if player_stats_df is not None and 'player_category' in player_stats_df.columns:
            category_counts = player_stats_df['player_category'].value_counts()
            player_categories = {
                'casual': int(category_counts.get('casual', 0)),
                'regular': int(category_counts.get('regular', 0)),
                'hardcore': int(category_counts.get('hardcore', 0))
            }
        
        # Advanced sentiment analysis
        sentiment_data = calculate_advanced_sentiment(reviews_df)
        
        # Real-time active players from streaming events
        active_players = 0
        recent_events = []
        if player_events_df is not None:
            # Get events from last hour
            if 'processed_at' in player_events_df.columns:
                one_hour_ago = datetime.now() - timedelta(hours=1)
                recent_events = player_events_df[
                    pd.to_datetime(player_events_df['processed_at']) >= one_hour_ago
                ]
            else:
                recent_events = player_events_df.tail(1000)  # Last 1000 events as fallback
            
            if len(recent_events) > 0:
                active_players = len(recent_events['player_id'].unique())
        
        # Player activity trends from streaming events
        activity_trends = {}
        if len(recent_events) > 0 and 'event_type' in recent_events.columns:
            activity_trends = recent_events['event_type'].value_counts().head(5).to_dict()
        
        # Server join/leave activity from logs
        server_activity = {'joins': 0, 'leaves': 0}
        if logs_df is not None and 'action' in logs_df.columns:
            server_activity = {
                'joins': len(logs_df[logs_df['action'].str.contains('joined', case=False, na=False)]),
                'leaves': len(logs_df[logs_df['action'].str.contains('left|disconnect', case=False, na=False)])
            }
        
        metrics = {
            'playtime_distribution': playtime_ranges,
            'player_categories': player_categories,
            'sentiment_score': sentiment_data['sentiment_score'],
            'sentiment_confidence': sentiment_data.get('confidence', 0.0),
            'sentiment_breakdown': {
                'positive': sentiment_data['positive_percentage'],
                'negative': sentiment_data['negative_percentage'], 
                'neutral': sentiment_data['neutral_percentage']
            },
            'sentiment_method': sentiment_data.get('method', 'unknown'),
            'total_analyzed_reviews': sentiment_data.get('total_analyzed', 0),
            'high_confidence_count': sentiment_data.get('high_confidence_reviews', 0),
            'real_time_active_players': active_players,
            'recent_activity_trends': activity_trends,
            'server_activity': server_activity,
            'data_sources': {
                'reviews_count': len(reviews_df) if reviews_df is not None else 0,
                'player_events_count': len(player_events_df) if player_events_df is not None else 0,
                'player_stats_count': len(player_stats_df) if player_stats_df is not None else 0,
                'logs_count': len(logs_df) if logs_df is not None else 0
            },
            'last_updated': datetime.now().isoformat()
        }
        
        logger.info(f"Player metrics calculated with streaming data. Active players: {active_players}, Sentiment: {sentiment_data['sentiment_score']:.1f}")
        
        return jsonify(metrics)
    except Exception as e:
        logger.error(f"Player metrics error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/server-health', methods=['GET'])
def server_health():
    """Real-time server health dan performance metrics dari streaming data"""
    try:
        # Get real-time streaming data
        logs_df = get_combined_data('logs', prefer_streaming=True, streaming_hours=24)
        server_logs_df = get_combined_data('logs', prefer_streaming=True, streaming_hours=1)  # Last hour for real-time
        game_metrics_df = get_combined_data('game_metrics', prefer_streaming=True, streaming_hours=1)
        configs_df = get_parquet_from_minio('configs.parquet')
        
        # Real-time log analysis
        log_levels = {}
        recent_errors = 0
        total_connections = 0
        top_actions = {}
        
        if logs_df is not None:
            # Overall log level distribution
            if 'level' in logs_df.columns:
                log_levels = logs_df['level'].value_counts().to_dict()
            
            # Recent errors (last hour)
            if server_logs_df is not None and 'level' in server_logs_df.columns:
                recent_errors = len(server_logs_df[server_logs_df['level'] == 'ERROR'])
            
            # Connection activity
            if 'action' in logs_df.columns:
                total_connections = len(logs_df[logs_df['action'].str.contains('joined|left', case=False, na=False)])
                top_actions = logs_df['action'].value_counts().head(5).to_dict()
        
        # Real-time server performance from game metrics
        server_performance = {
            'avg_response_time': 0,
            'crash_rate': 0,
            'concurrent_players': 0,
            'health_status': 'unknown'
        }
        
        if game_metrics_df is not None and len(game_metrics_df) > 0:
            # Calculate average metrics from recent data
            recent_metrics = game_metrics_df.tail(20)  # Last 20 metrics
            
            if 'concurrent_players' in recent_metrics.columns:
                server_performance['concurrent_players'] = int(recent_metrics['concurrent_players'].mean())
            
            if 'crash_rate_percent' in recent_metrics.columns:
                server_performance['crash_rate'] = float(recent_metrics['crash_rate_percent'].mean())
            
            if 'health_status' in recent_metrics.columns:
                # Get most recent health status
                latest_status = recent_metrics['health_status'].iloc[-1]
                server_performance['health_status'] = latest_status
        
        # Server configuration info
        server_config = {
            'active_servers': 0,
            'avg_max_players': 0,
            'total_capacity': 0
        }
        
        if configs_df is not None:
            server_config['active_servers'] = len(configs_df)
            if 'game_server_max_players' in configs_df.columns:
                avg_max = float(configs_df['game_server_max_players'].mean())
                server_config['avg_max_players'] = round(avg_max, 0)
                server_config['total_capacity'] = int(configs_df['game_server_max_players'].sum())
        
        # Real-time alerts
        alerts = []
        if recent_errors > 10:
            alerts.append({
                'type': 'error',
                'message': f'High error rate: {recent_errors} errors in last hour',
                'severity': 'high'
            })
        
        if server_performance['crash_rate'] > 2.0:
            alerts.append({
                'type': 'performance',
                'message': f'High crash rate: {server_performance["crash_rate"]:.1f}%',
                'severity': 'high'
            })
        
        if server_performance['health_status'] == 'poor':
            alerts.append({
                'type': 'health',
                'message': 'Server health status is poor',
                'severity': 'high'
            })
        
        health = {
            'log_levels': log_levels,
            'recent_errors': recent_errors,
            'total_connections': total_connections,
            'top_actions': top_actions,
            'server_performance': server_performance,
            'server_config': server_config,
            'real_time_alerts': alerts,
            'data_freshness': {
                'logs_count': len(logs_df) if logs_df is not None else 0,
                'recent_logs_count': len(server_logs_df) if server_logs_df is not None else 0,
                'metrics_count': len(game_metrics_df) if game_metrics_df is not None else 0,
                'last_updated': datetime.now().isoformat()
            }
        }
        
        logger.info(f"Server health calculated with streaming data. Recent errors: {recent_errors}, Health: {server_performance['health_status']}")
        
        return jsonify(health)
    except Exception as e:
        logger.error(f"Server health error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/game-performance', methods=['GET'])
def game_performance():
    """Real-time game performance metrics dari streaming data"""
    try:
        # Get streaming and warehouse data
        reviews_df = get_combined_data('reviews', prefer_streaming=True, streaming_hours=24)
        game_metrics_df = get_combined_data('game_metrics', prefer_streaming=True, streaming_hours=24)
        player_events_df = get_combined_data('player_events', prefer_streaming=True, streaming_hours=24)
        
        # Game performance from real-time metrics
        game_performance_data = {
            'concurrent_players': 0,
            'avg_crash_rate': 0,
            'games_with_poor_health': 0,
            'total_active_games': 0
        }
        
        if game_metrics_df is not None and len(game_metrics_df) > 0:
            # Recent game metrics analysis
            recent_metrics = game_metrics_df.tail(100)  # Last 100 metrics
            
            if 'concurrent_players' in recent_metrics.columns:
                game_performance_data['concurrent_players'] = int(recent_metrics['concurrent_players'].mean())
            
            if 'crash_rate_percent' in recent_metrics.columns:
                game_performance_data['avg_crash_rate'] = float(recent_metrics['crash_rate_percent'].mean())
            
            if 'health_status' in recent_metrics.columns:
                game_performance_data['games_with_poor_health'] = len(recent_metrics[recent_metrics['health_status'] == 'poor'])
            
            if 'game_id' in recent_metrics.columns:
                game_performance_data['total_active_games'] = len(recent_metrics['game_id'].unique())
        
        # Top games by activity from player events
        top_games_by_activity = {}
        if player_events_df is not None and 'game_id' in player_events_df.columns:
            game_activity = player_events_df['game_id'].value_counts().head(10)
            top_games_by_activity = game_activity.to_dict()
        
        # Game categories performance (simulated from reviews)
        top_games = {}
        category_sentiment = {}
        avg_helpful_ratio = 0
        recent_reviews = 0
        
        if reviews_df is not None:
            # Top reviewed games (simulasi berdasarkan review_id patterns)
            if 'review_id' in reviews_df.columns:
                top_games = {
                    'Action Games': len(reviews_df[reviews_df['review_id'].str.contains('00[1-3]', na=False)]),
                    'RPG Games': len(reviews_df[reviews_df['review_id'].str.contains('00[4-6]', na=False)]),
                    'Strategy Games': len(reviews_df[reviews_df['review_id'].str.contains('00[7-9]', na=False)])
                }
            
            # Average ratings from reviews
            if 'helpful_votes' in reviews_df.columns and 'total_votes' in reviews_df.columns:
                total_helpful = reviews_df['helpful_votes'].sum()
                total_votes = reviews_df['total_votes'].sum()
                avg_helpful_ratio = float(total_helpful / total_votes) if total_votes > 0 else 0
            
            # Recent reviews count (streaming data)
            recent_reviews = len(reviews_df.tail(200))  # Recent activity
            
            # Calculate sentiment by game category
            if SENTIMENT_ANALYZER_AVAILABLE and sentiment_analyzer and len(top_games) > 0:
                try:
                    for category in top_games.keys():
                        pattern = ('00[1-3]' if 'Action' in category else 
                                 '00[4-6]' if 'RPG' in category else '00[7-9]')
                        category_sample = reviews_df[reviews_df['review_id'].str.contains(pattern, na=False)].head(50)
                        
                        if len(category_sample) > 0:
                            cat_sentiment = calculate_advanced_sentiment(category_sample)
                            category_sentiment[category] = cat_sentiment['sentiment_score']
                        else:
                            category_sentiment[category] = 50.0
                except Exception as e:
                    logger.warning(f"Category sentiment analysis failed: {e}")
        
        # Player engagement metrics from events
        player_engagement = {
            'total_sessions': 0,
            'avg_session_length': 0,
            'achievement_unlocks': 0,
            'purchases': 0
        }
        
        if player_events_df is not None and 'event_type' in player_events_df.columns:
            event_counts = player_events_df['event_type'].value_counts()
            player_engagement = {
                'total_sessions': int(event_counts.get('game_start', 0)),
                'achievement_unlocks': int(event_counts.get('achievement_unlock', 0)),
                'purchases': int(event_counts.get('purchase', 0)),
                'total_events': len(player_events_df)
            }
            
            # Calculate average session length if we have session data
            if 'session_id' in player_events_df.columns:
                unique_sessions = len(player_events_df['session_id'].unique())
                if unique_sessions > 0:
                    player_engagement['avg_session_length'] = round(len(player_events_df) / unique_sessions, 1)
        
        performance = {
            'real_time_performance': game_performance_data,
            'top_games_by_reviews': top_games,
            'top_games_by_activity': top_games_by_activity,
            'player_engagement': player_engagement,
            'avg_helpful_ratio': round(avg_helpful_ratio * 100, 2),
            'recent_reviews_count': recent_reviews,
            'category_sentiment_scores': category_sentiment,
            'data_sources': {
                'reviews_count': len(reviews_df) if reviews_df is not None else 0,
                'game_metrics_count': len(game_metrics_df) if game_metrics_df is not None else 0,
                'player_events_count': len(player_events_df) if player_events_df is not None else 0
            },
            'last_updated': datetime.now().isoformat()
        }
        
        logger.info(f"Game performance calculated with streaming data. Active games: {game_performance_data['total_active_games']}, Concurrent players: {game_performance_data['concurrent_players']}")
        
        return jsonify(performance)
    except Exception as e:
        logger.error(f"Game performance error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/sentiment/analyze', methods=['POST'])
def analyze_single_review():
    """Endpoint untuk menganalisis sentiment single review"""
    try:
        if not SENTIMENT_ANALYZER_AVAILABLE:
            return jsonify({'error': 'Advanced sentiment analysis not available'}), 503
        
        data = request.get_json()
        if not data or 'text' not in data:
            return jsonify({'error': 'Missing text field'}), 400
        
        result = sentiment_analyzer.analyze_sentiment(data['text'])
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Single review analysis error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'sentiment_analyzer': 'enabled' if SENTIMENT_ANALYZER_AVAILABLE else 'disabled',
        'lightweight_mode': LIGHTWEIGHT_MODE,
        'cache_entries': len(sentiment_cache)
    })

@app.route('/api/config/performance', methods=['GET'])
def get_performance_config():
    """Get current performance configuration"""
    return jsonify({
        'lightweight_mode': LIGHTWEIGHT_MODE,
        'sentiment_analyzer_available': SENTIMENT_ANALYZER_AVAILABLE,
        'cache_size': len(sentiment_cache),
        'max_cache_size': 10,
        'sample_size': 200 if LIGHTWEIGHT_MODE else 1000,
        'use_transformers': False
    })

@app.route('/api/config/performance', methods=['POST'])
def update_performance_config():
    """Update performance configuration"""
    global LIGHTWEIGHT_MODE, sentiment_cache
    
    try:
        data = request.get_json()
        
        if 'lightweight_mode' in data:
            LIGHTWEIGHT_MODE = bool(data['lightweight_mode'])
            logger.info(f"🔧 Lightweight mode set to: {LIGHTWEIGHT_MODE}")
        
        if 'clear_cache' in data and data['clear_cache']:
            sentiment_cache.clear()
            logger.info("🗑️ Sentiment cache cleared")
        
        return jsonify({
            'success': True,
            'lightweight_mode': LIGHTWEIGHT_MODE,
            'cache_cleared': 'clear_cache' in data and data['clear_cache'],
            'message': 'Performance configuration updated'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/config/cache/clear', methods=['POST'])
def clear_cache():
    """Clear sentiment analysis cache"""
    global sentiment_cache
    sentiment_cache.clear()
    logger.info("🗑️ Sentiment analysis cache cleared")
    
    return jsonify({
        'success': True,
        'message': 'Cache cleared successfully',
        'cache_size': len(sentiment_cache)
    })

# New streaming data endpoints
@app.route('/api/streaming/player-events', methods=['GET'])
def get_player_events():
    """Get real-time player events dari streaming data"""
    try:
        hours = request.args.get('hours', 1, type=int)
        limit = request.args.get('limit', 100, type=int)
        
        player_events_df = get_streaming_data('player_events', hours_back=hours)
        
        if player_events_df is None or len(player_events_df) == 0:
            return jsonify({
                'events': [],
                'total_count': 0,
                'message': 'No player events found'
            })
        
        # Limit results and sort by timestamp
        if 'processed_at' in player_events_df.columns:
            player_events_df = player_events_df.sort_values('processed_at', ascending=False)
        
        events_sample = player_events_df.head(limit)
        
        return jsonify({
            'events': events_sample.to_dict('records'),
            'total_count': len(player_events_df),
            'sample_size': len(events_sample),
            'hours_back': hours,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Player events streaming error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/streaming/game-metrics', methods=['GET'])
def get_game_metrics():
    """Get real-time game metrics dari streaming data"""
    try:
        hours = request.args.get('hours', 1, type=int)
        limit = request.args.get('limit', 50, type=int)
        
        game_metrics_df = get_streaming_data('game_metrics', hours_back=hours)
        
        if game_metrics_df is None or len(game_metrics_df) == 0:
            return jsonify({
                'metrics': [],
                'total_count': 0,
                'message': 'No game metrics found'
            })
        
        # Sort by timestamp and limit results
        if 'processed_at' in game_metrics_df.columns:
            game_metrics_df = game_metrics_df.sort_values('processed_at', ascending=False)
        
        metrics_sample = game_metrics_df.head(limit)
        
        # Calculate summary statistics
        summary = {}
        if len(game_metrics_df) > 0:
            if 'concurrent_players' in game_metrics_df.columns:
                summary['avg_concurrent_players'] = float(game_metrics_df['concurrent_players'].mean())
                summary['max_concurrent_players'] = int(game_metrics_df['concurrent_players'].max())
            
            if 'crash_rate_percent' in game_metrics_df.columns:
                summary['avg_crash_rate'] = float(game_metrics_df['crash_rate_percent'].mean())
                summary['max_crash_rate'] = float(game_metrics_df['crash_rate_percent'].max())
            
            if 'health_status' in game_metrics_df.columns:
                health_counts = game_metrics_df['health_status'].value_counts()
                summary['health_distribution'] = health_counts.to_dict()
        
        return jsonify({
            'metrics': metrics_sample.to_dict('records'),
            'summary': summary,
            'total_count': len(game_metrics_df),
            'sample_size': len(metrics_sample),
            'hours_back': hours,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Game metrics streaming error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/streaming/reviews', methods=['GET'])
def get_streaming_reviews():
    """Get real-time reviews dari streaming data"""
    try:
        hours = request.args.get('hours', 24, type=int)
        limit = request.args.get('limit', 50, type=int)
        
        reviews_df = get_streaming_data('game_reviews', hours_back=hours)
        
        if reviews_df is None or len(reviews_df) == 0:
            return jsonify({
                'reviews': [],
                'total_count': 0,
                'message': 'No streaming reviews found'
            })
        
        # Sort by timestamp and limit results
        if 'processed_at' in reviews_df.columns:
            reviews_df = reviews_df.sort_values('processed_at', ascending=False)
        
        reviews_sample = reviews_df.head(limit)
        
        # Calculate review statistics
        stats = {}
        if len(reviews_df) > 0:
            if 'sentiment_indicator' in reviews_df.columns:
                sentiment_counts = reviews_df['sentiment_indicator'].value_counts()
                stats['sentiment_distribution'] = sentiment_counts.to_dict()
            
            if 'rating' in reviews_df.columns:
                stats['avg_rating'] = float(reviews_df['rating'].mean())
                stats['rating_distribution'] = reviews_df['rating'].value_counts().to_dict()
            
            if 'helpful_votes' in reviews_df.columns:
                stats['total_helpful_votes'] = int(reviews_df['helpful_votes'].sum())
                stats['avg_helpful_votes'] = float(reviews_df['helpful_votes'].mean())
        
        return jsonify({
            'reviews': reviews_sample.to_dict('records'),
            'statistics': stats,
            'total_count': len(reviews_df),
            'sample_size': len(reviews_sample),
            'hours_back': hours,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Streaming reviews error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/streaming/status', methods=['GET'])
def streaming_status():
    """Get status of streaming data availability"""
    try:
        status = {
            'streaming_bucket_available': False,
            'data_types': {},
            'total_files': 0,
            'last_activity': None
        }
        
        # Check if streaming bucket exists
        try:
            if client.bucket_exists(STREAMING_BUCKET):
                status['streaming_bucket_available'] = True
                
                # Check each data type
                data_types = ['player_events', 'game_reviews', 'server_logs', 'player_stats', 'game_metrics']
                
                for data_type in data_types:
                    try:
                        prefix = f"streaming/{data_type}/"
                        objects = list(client.list_objects(STREAMING_BUCKET, prefix=prefix, recursive=True))
                        
                        file_count = len(objects)
                        status['data_types'][data_type] = {
                            'file_count': file_count,
                            'available': file_count > 0
                        }
                        
                        # Get latest file timestamp
                        if objects:
                            latest_obj = max(objects, key=lambda x: x.last_modified)
                            status['data_types'][data_type]['last_modified'] = latest_obj.last_modified.isoformat()
                            
                            if status['last_activity'] is None or latest_obj.last_modified > status['last_activity']:
                                status['last_activity'] = latest_obj.last_modified.isoformat()
                        
                        status['total_files'] += file_count
                        
                    except Exception as e:
                        logger.warning(f"Could not check {data_type}: {e}")
                        status['data_types'][data_type] = {
                            'file_count': 0,
                            'available': False,
                            'error': str(e)
                        }
                        
        except Exception as e:
            logger.error(f"Could not check streaming bucket: {e}")
            status['error'] = str(e)
        
        return jsonify(status)
        
    except Exception as e:
        logger.error(f"Streaming status error: {e}")
        return jsonify({'error': str(e)}), 500

# ==========================================
# CLUSTERING API ENDPOINTS
# ==========================================

@app.route('/api/clustering/games', methods=['GET'])
def get_clustered_games():
    """Get clustered games data with filtering and pagination"""
    try:
        # Get query parameters
        cluster_id = request.args.get('cluster_id', type=int)
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        include_features = request.args.get('include_features', 'false').lower() == 'true'
        
        logger.info(f"Getting clustered games: cluster_id={cluster_id}, limit={limit}, offset={offset}")
        
        # Load latest clusters data
        try:
            clusters_df = get_parquet_from_minio('latest/games_clusters_latest.parquet', bucket=CLUSTERS_BUCKET)
            if clusters_df is None:
                return jsonify({'error': 'No clustering data available'}), 404
        except Exception as e:
            logger.error(f"Error loading clusters data: {e}")
            return jsonify({'error': 'Failed to load clustering data'}), 500
        
        # Filter by cluster if specified
        if cluster_id is not None:
            clusters_df = clusters_df[clusters_df['cluster'] == cluster_id]
            
        # Select columns
        base_columns = ['app_id', 'title', 'rating', 'positive_ratio', 'user_reviews', 'price_final', 'cluster']
        if include_features:
            feature_columns = [col for col in clusters_df.columns if '_encoded' in col or '_count' in col or '_category' in col or '_numeric' in col or '_log' in col or '_year' in col]
            columns = base_columns + feature_columns
        else:
            columns = base_columns
            
        # Filter columns that exist
        available_columns = [col for col in columns if col in clusters_df.columns]
        result_df = clusters_df[available_columns]
        
        # Apply pagination
        total_count = len(result_df)
        result_df = result_df.iloc[offset:offset + limit]
        
        # Convert to response format
        games = result_df.to_dict('records')
        
        response = {
            'games': games,
            'pagination': {
                'total': total_count,
                'limit': limit,
                'offset': offset,
                'has_more': offset + limit < total_count
            },
            'cluster_filter': cluster_id,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting clustered games: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/clustering/analysis', methods=['GET'])
def get_cluster_analysis():
    """Get detailed cluster analysis and statistics"""
    try:
        logger.info("Getting cluster analysis")
        
        # Load latest cluster analysis
        try:
            data = client.get_object(CLUSTERS_BUCKET, 'latest/cluster_analysis_latest.json')
            analysis_data = json.loads(data.read().decode('utf-8'))
        except Exception as e:
            logger.error(f"Error loading cluster analysis: {e}")
            return jsonify({'error': 'No cluster analysis available'}), 404
        
        # Add summary statistics
        if 'cluster_analysis' in analysis_data:
            cluster_analysis = analysis_data['cluster_analysis']
            
            # Calculate summary stats
            total_games = sum(cluster['game_count'] for cluster in cluster_analysis)
            avg_price_overall = sum(cluster['avg_price'] * cluster['game_count'] for cluster in cluster_analysis) / total_games if total_games > 0 else 0
            
            summary = {
                'total_games': total_games,
                'total_clusters': len(cluster_analysis),
                'avg_price_overall': round(avg_price_overall, 2),
                'largest_cluster': max(cluster_analysis, key=lambda x: x['game_count']) if cluster_analysis else None,
                'highest_rated_cluster': max(cluster_analysis, key=lambda x: x['avg_positive_ratio']) if cluster_analysis else None
            }
            
            analysis_data['summary'] = summary
        
        return jsonify(analysis_data)
        
    except Exception as e:
        logger.error(f"Error getting cluster analysis: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/games/search', methods=['GET'])
def search_games():
    """Search games by title/name"""
    try:
        # Get query parameters
        query = request.args.get('q', '').strip()
        limit = request.args.get('limit', 10, type=int)
        
        if not query:
            return jsonify({'error': 'Query parameter "q" is required'}), 400
        
        logger.info(f"Searching games with query: '{query}'")
        
        # Load clusters data to get games list
        try:
            clusters_df = get_parquet_from_minio('latest/games_clusters_latest.parquet', bucket=CLUSTERS_BUCKET)
            if clusters_df is None:
                # Fallback to games.parquet if clusters not available
                clusters_df = get_parquet_from_minio('games.parquet')
                if clusters_df is None:
                    return jsonify({'error': 'No games data available'}), 404
        except Exception as e:
            logger.error(f"Error loading games data: {e}")
            return jsonify({'error': 'Failed to load games data'}), 500
        
        # Search for games by title (case-insensitive)
        query_lower = query.lower()
        matching_games = clusters_df[
            clusters_df['title'].str.lower().str.contains(query_lower, na=False, regex=False)
        ]
        
        if matching_games.empty:
            return jsonify({
                'games': [],
                'total_count': 0,
                'query': query,
                'message': 'No games found matching the query'
            })
        
        # Sort by relevance (exact matches first, then partial matches)
        matching_games = matching_games.copy()
        matching_games['relevance_score'] = matching_games['title'].str.lower().apply(
            lambda x: 100 if x == query_lower else (50 if x.startswith(query_lower) else 10)
        )
        matching_games = matching_games.sort_values(['relevance_score', 'user_reviews'], ascending=[False, False])
        
        # Limit results
        result_games = matching_games.head(limit)
        
        # Prepare response
        games_list = []
        for _, game in result_games.iterrows():
            game_dict = {
                'app_id': int(game['app_id']),
                'title': game['title'],
                'rating': game.get('rating', 'N/A'),
                'user_reviews': int(game.get('user_reviews', 0)),
                'price_final': float(game.get('price_final', 0)),
                'relevance_score': float(game['relevance_score'])
            }
            
            # Add cluster info if available
            if 'cluster' in game:
                game_dict['cluster'] = int(game['cluster'])
            if 'positive_ratio' in game:
                game_dict['positive_ratio'] = float(game['positive_ratio'])
                
            games_list.append(game_dict)
        
        response = {
            'games': games_list,
            'total_count': len(matching_games),
            'returned_count': len(games_list),
            'query': query,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error searching games: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/recommendations/<game_title>', methods=['GET'])
def get_recommendations_by_title(game_title):
    """Get game recommendations by game title (more user-friendly endpoint)"""
    try:
        logger.info(f"Getting recommendations for game title: '{game_title}'")
        
        # First, search for the game by title
        # Load clusters data
        try:
            clusters_df = get_parquet_from_minio('latest/games_clusters_latest.parquet', bucket=CLUSTERS_BUCKET)
            if clusters_df is None:
                return jsonify({'error': 'No clustering data available'}), 404
        except Exception as e:
            logger.error(f"Error loading clusters data: {e}")
            return jsonify({'error': 'Failed to load clustering data'}), 500
        
        # Search for the game (case-insensitive, partial match)
        game_title_lower = game_title.lower().strip()
        matching_games = clusters_df[
            clusters_df['title'].str.lower().str.contains(game_title_lower, na=False, regex=False)
        ]
        
        if matching_games.empty:
            return jsonify({
                'error': 'Game not found',
                'searched_title': game_title,
                'suggestions': 'Try searching with partial game name or check spelling'
            }), 404
        
        # Get the best match (prioritize exact matches, then by popularity)
        matching_games = matching_games.copy()
        matching_games['match_score'] = matching_games['title'].str.lower().apply(
            lambda x: 100 if x == game_title_lower else (80 if x.startswith(game_title_lower) else 50)
        )
        best_match = matching_games.sort_values(['match_score', 'user_reviews'], ascending=[False, False]).iloc[0]
        
        game_id = int(best_match['app_id'])
        
        # Now get recommendations using the existing logic
        limit = request.args.get('limit', 8, type=int)
        same_cluster_only = request.args.get('same_cluster_only', 'false').lower() == 'true'
        
        reference_cluster = best_match['cluster']
        
        # Get recommendation candidates
        if same_cluster_only:
            candidates = clusters_df[
                (clusters_df['cluster'] == reference_cluster) & 
                (clusters_df['app_id'] != game_id)
            ]
        else:
            candidates = clusters_df[clusters_df['app_id'] != game_id]
        
        if candidates.empty:
            return jsonify({
                'recommendations': [],
                'game_info': {
                    'app_id': game_id,
                    'title': best_match['title'],
                    'cluster': int(best_match['cluster']),
                    'rating': best_match['rating'],
                    'positive_ratio': float(best_match['positive_ratio']),
                    'user_reviews': int(best_match['user_reviews']),
                    'price_final': float(best_match['price_final'])
                }
            })
        
        # Score recommendations based on similarity
        ref_price = best_match['price_final']
        ref_rating = best_match['positive_ratio']
        ref_reviews = best_match['user_reviews']
        
        # Calculate similarity scores
        candidates = candidates.copy()
        candidates['price_similarity'] = 1 - abs(candidates['price_final'] - ref_price) / max(candidates['price_final'].max(), ref_price, 1)
        candidates['rating_similarity'] = 1 - abs(candidates['positive_ratio'] - ref_rating) / 100
        candidates['popularity_factor'] = (candidates['user_reviews'] + 1).apply(lambda x: min(x / (ref_reviews + 1), 2))
        
        # Combined similarity score
        candidates['similarity_score'] = (
            candidates['price_similarity'] * 0.3 +
            candidates['rating_similarity'] * 0.4 +
            candidates['popularity_factor'] * 0.3
        )
        
        # Sort by similarity and get top recommendations
        recommendations = candidates.nlargest(limit, 'similarity_score')
        
        # Prepare response
        rec_list = []
        for _, game in recommendations.iterrows():
            rec_list.append({
                'app_id': int(game['app_id']),
                'title': game['title'],
                'rating': game['rating'],
                'positive_ratio': float(game['positive_ratio']),
                'user_reviews': int(game['user_reviews']),
                'price_final': float(game['price_final']),
                'cluster': int(game['cluster']),
                'similarity_score': float(game['similarity_score'])
            })
        
        response = {
            'recommendations': rec_list,
            'game_info': {
                'app_id': game_id,
                'title': best_match['title'],
                'cluster': int(best_match['cluster']),
                'rating': best_match['rating'],
                'positive_ratio': float(best_match['positive_ratio']),
                'user_reviews': int(best_match['user_reviews']),
                'price_final': float(best_match['price_final'])
            },
            'search_info': {
                'searched_title': game_title,
                'matched_title': best_match['title'],
                'match_score': float(best_match['match_score']),
                'total_matches': len(matching_games)
            },
            'parameters': {
                'same_cluster_only': same_cluster_only,
                'limit': limit,
                'reference_cluster': int(reference_cluster)
            },
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting recommendations by title: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/clustering/recommendations', methods=['GET'])
def get_game_recommendations():
    """Get game recommendations based on clustering similarity"""
    try:
        # Get query parameters
        game_id = request.args.get('game_id', type=int)
        limit = request.args.get('limit', 10, type=int)
        same_cluster_only = request.args.get('same_cluster_only', 'true').lower() == 'true'
        
        if not game_id:
            return jsonify({'error': 'game_id parameter is required'}), 400
        
        logger.info(f"Getting recommendations for game_id={game_id}, limit={limit}")
        
        # Load clusters data
        try:
            clusters_df = get_parquet_from_minio('latest/games_clusters_latest.parquet', bucket=CLUSTERS_BUCKET)
            if clusters_df is None:
                return jsonify({'error': 'No clustering data available'}), 404
        except Exception as e:
            logger.error(f"Error loading clusters data: {e}")
            return jsonify({'error': 'Failed to load clustering data'}), 500
        
        # Find the reference game
        reference_game = clusters_df[clusters_df['app_id'] == game_id]
        if reference_game.empty:
            return jsonify({'error': f'Game with ID {game_id} not found'}), 404
        
        reference_cluster = reference_game.iloc[0]['cluster']
        
        # Get recommendation candidates
        if same_cluster_only:
            candidates = clusters_df[
                (clusters_df['cluster'] == reference_cluster) & 
                (clusters_df['app_id'] != game_id)
            ]
        else:
            candidates = clusters_df[clusters_df['app_id'] != game_id]
        
        if candidates.empty:
            return jsonify({'recommendations': [], 'reference_game': reference_game.iloc[0].to_dict()})
        
        # Score recommendations based on similarity
        ref_price = reference_game.iloc[0]['price_final']
        ref_rating = reference_game.iloc[0]['positive_ratio']
        ref_reviews = reference_game.iloc[0]['user_reviews']
        
        # Calculate similarity scores
        candidates = candidates.copy()
        candidates['price_similarity'] = 1 - abs(candidates['price_final'] - ref_price) / max(candidates['price_final'].max(), ref_price, 1)
        candidates['rating_similarity'] = 1 - abs(candidates['positive_ratio'] - ref_rating) / 100
        candidates['popularity_factor'] = (candidates['user_reviews'] + 1).apply(lambda x: min(x / (ref_reviews + 1), 2))
        
        # Combined similarity score
        candidates['similarity_score'] = (
            candidates['price_similarity'] * 0.3 +
            candidates['rating_similarity'] * 0.4 +
            candidates['popularity_factor'] * 0.3
        )
        
        # Sort by similarity and get top recommendations
        recommendations = candidates.nlargest(limit, 'similarity_score')
        
        # Prepare response
        rec_list = []
        for _, game in recommendations.iterrows():
            rec_list.append({
                'app_id': int(game['app_id']),
                'title': game['title'],
                'rating': game['rating'],
                'positive_ratio': int(game['positive_ratio']),
                'user_reviews': int(game['user_reviews']),
                'price_final': float(game['price_final']),
                'cluster': int(game['cluster']),
                'similarity_score': float(game['similarity_score'])
            })
        
        response = {
            'recommendations': rec_list,
            'reference_game': {
                'app_id': int(reference_game.iloc[0]['app_id']),
                'title': reference_game.iloc[0]['title'],
                'cluster': int(reference_game.iloc[0]['cluster']),
                'rating': reference_game.iloc[0]['rating'],
                'positive_ratio': int(reference_game.iloc[0]['positive_ratio']),
                'price_final': float(reference_game.iloc[0]['price_final'])
            },
            'parameters': {
                'same_cluster_only': same_cluster_only,
                'limit': limit,
                'reference_cluster': int(reference_cluster)
            },
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/clustering/status', methods=['GET'])
def get_clustering_status():
    """Get status of clustering system"""
    try:
        status = {
            'timestamp': datetime.now().isoformat(),
            'clustering_status': 'unknown',
            'latest_clusters': None,
            'latest_analysis': None,
            'data_freshness': {}
        }
        
        # Check if clusters bucket exists
        try:
            if not client.bucket_exists(CLUSTERS_BUCKET):
                status['clustering_status'] = 'no_data'
                status['message'] = 'Clusters bucket does not exist'
                return jsonify(status)
        except Exception as e:
            status['clustering_status'] = 'error'
            status['error'] = str(e)
            return jsonify(status)
        
        # Check latest clusters data
        try:
            objects = list(client.list_objects(CLUSTERS_BUCKET, prefix='latest/', recursive=True))
            
            latest_clusters_obj = None
            latest_analysis_obj = None
            
            for obj in objects:
                if obj.object_name.endswith('games_clusters_latest.parquet'):
                    latest_clusters_obj = obj
                elif obj.object_name.endswith('cluster_analysis_latest.json'):
                    latest_analysis_obj = obj
            
            if latest_clusters_obj:
                status['latest_clusters'] = {
                    'file': latest_clusters_obj.object_name,
                    'last_modified': latest_clusters_obj.last_modified.isoformat(),
                    'size': latest_clusters_obj.size
                }
                
                # Check data freshness - be more tolerant for real-time systems
                time_diff = datetime.now() - latest_clusters_obj.last_modified.replace(tzinfo=None)
                status['data_freshness']['clusters'] = {
                    'hours_old': time_diff.total_seconds() / 3600,
                    'minutes_old': time_diff.total_seconds() / 60,
                    'is_fresh': time_diff < timedelta(hours=12),  # Increased from 6 to 12 hours
                    'is_very_fresh': time_diff < timedelta(minutes=10),  # Added very fresh indicator
                    'last_update': latest_clusters_obj.last_modified.isoformat()
                }
            
            if latest_analysis_obj:
                status['latest_analysis'] = {
                    'file': latest_analysis_obj.object_name,
                    'last_modified': latest_analysis_obj.last_modified.isoformat(),
                    'size': latest_analysis_obj.size
                }
        
        except Exception as e:
            status['clustering_status'] = 'error'
            status['error'] = f'Error checking latest data: {str(e)}'
            return jsonify(status)
        
        # Determine overall status
        if status['latest_clusters'] and status['latest_analysis']:
            if status['data_freshness'].get('clusters', {}).get('is_fresh', False):
                status['clustering_status'] = 'healthy'
            else:
                status['clustering_status'] = 'stale'
        else:
            status['clustering_status'] = 'no_data'
        
        return jsonify(status)
        
    except Exception as e:
        logger.error(f"Error getting clustering status: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("🚀 Starting Gaming Analytics API with Advanced Sentiment Analysis")
    logger.info(f"📊 Sentiment Analysis: {'Advanced NLP' if SENTIMENT_ANALYZER_AVAILABLE else 'Basic Fallback'}")
    # Production mode untuk stabilitas dalam orchestrator
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True) 