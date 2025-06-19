from flask import Flask, jsonify
import pandas as pd
from minio import Minio
import duckdb
from io import BytesIO
import os
import logging

# Import sentiment analyzer
try:
    from sentiment_analyzer import GameReviewSentimentAnalyzer, analyze_review_dataframe
    SENTIMENT_ANALYZER_AVAILABLE = True
except ImportError:
    SENTIMENT_ANALYZER_AVAILABLE = False
    logging.warning("⚠️ Advanced sentiment analyzer not available. Using fallback method.")

app = Flask(__name__)

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

# Global sentiment analyzer instance
sentiment_analyzer = None
if SENTIMENT_ANALYZER_AVAILABLE:
    try:
        sentiment_analyzer = GameReviewSentimentAnalyzer(use_transformers=False)  # Set to True for more accuracy
        logger.info("✅ Advanced sentiment analyzer initialized")
    except Exception as e:
        logger.warning(f"⚠️ Could not initialize sentiment analyzer: {e}")
        SENTIMENT_ANALYZER_AVAILABLE = False

def get_parquet_from_minio(filename):
    """Download Parquet file from MinIO and return as DataFrame"""
    try:
        data = client.get_object(WAREHOUSE_BUCKET, filename)
        df = pd.read_parquet(BytesIO(data.read()))
        return df
    except Exception as e:
        logger.error(f"Error loading {filename}: {e}")
        return None

def calculate_advanced_sentiment(reviews_df):
    """
    Calculate advanced sentiment analysis for reviews
    
    Returns:
        Dict with comprehensive sentiment metrics
    """
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
            
            # Limit to first 1000 reviews for performance (can be adjusted)
            sample_size = min(1000, len(reviews_df))
            sample_df = reviews_df.head(sample_size)
            
            # Analyze sentiments
            analyzed_df = analyze_review_dataframe(
                sample_df, 
                text_column, 
                use_transformers=False  # Set to True for transformer models
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
    """Overview metrics untuk dashboard utama"""
    try:
        # Load data
        games_df = get_parquet_from_minio('games.parquet')
        reviews_df = get_parquet_from_minio('reviews.parquet')
        logs_df = get_parquet_from_minio('logs.parquet')
        
        overview = {
            'total_games': len(games_df) if games_df is not None else 0,
            'total_reviews': len(reviews_df) if reviews_df is not None else 0,
            'total_log_entries': len(logs_df) if logs_df is not None else 0,
            'avg_playtime': float(reviews_df['playtime_hours'].mean()) if reviews_df is not None else 0,
            'sentiment_analysis_enabled': SENTIMENT_ANALYZER_AVAILABLE
        }
        
        return jsonify(overview)
    except Exception as e:
        logger.error(f"Dashboard overview error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/player-metrics', methods=['GET'])
def player_metrics():
    """Enhanced player metrics dengan advanced sentiment analysis"""
    try:
        reviews_df = get_parquet_from_minio('reviews.parquet')
        logs_df = get_parquet_from_minio('logs.parquet')
        
        # Playtime distribution
        if reviews_df is not None:
            playtime_ranges = {
                '0-10h': len(reviews_df[reviews_df['playtime_hours'] <= 10]),
                '10-50h': len(reviews_df[(reviews_df['playtime_hours'] > 10) & (reviews_df['playtime_hours'] <= 50)]),
                '50-100h': len(reviews_df[(reviews_df['playtime_hours'] > 50) & (reviews_df['playtime_hours'] <= 100)]),
                '100h+': len(reviews_df[reviews_df['playtime_hours'] > 100])
            }
        else:
            playtime_ranges = {}
        
        # Advanced sentiment analysis
        sentiment_data = calculate_advanced_sentiment(reviews_df)
        
        # Active players from logs
        if logs_df is not None:
            active_players = len(logs_df[logs_df['action'] == 'Player joined server']['player_id'].unique())
        else:
            active_players = 0
        
        metrics = {
            'playtime_distribution': playtime_ranges,
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
            'active_players': active_players
        }
        
        logger.info(f"Player metrics calculated. Sentiment: {sentiment_data['sentiment_score']:.1f} ({sentiment_data.get('method', 'unknown')} method)")
        
        return jsonify(metrics)
    except Exception as e:
        logger.error(f"Player metrics error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/server-health', methods=['GET'])
def server_health():
    """Metrics terkait server health dan performance"""
    try:
        logs_df = get_parquet_from_minio('logs.parquet')
        configs_df = get_parquet_from_minio('configs.parquet')
        
        if logs_df is not None:
            # Log level distribution
            log_levels = logs_df['level'].value_counts().to_dict()
            
            # Server events
            recent_errors = len(logs_df[logs_df['level'] == 'ERROR'])
            total_connections = len(logs_df[logs_df['action'].str.contains('joined|left', case=False, na=False)])
            
            # Top server actions
            top_actions = logs_df['action'].value_counts().head(5).to_dict()
        else:
            log_levels = {}
            recent_errors = 0
            total_connections = 0
            top_actions = {}
            
        if configs_df is not None:
            active_servers = len(configs_df)
            avg_max_players = float(configs_df['game_server_max_players'].mean()) if 'game_server_max_players' in configs_df.columns else 0
        else:
            active_servers = 0
            avg_max_players = 0
            
        health = {
            'log_levels': log_levels,
            'recent_errors': recent_errors,
            'total_connections': total_connections,
            'top_actions': top_actions,
            'active_servers': active_servers,
            'avg_max_players': round(avg_max_players, 0)
        }
        
        return jsonify(health)
    except Exception as e:
        logger.error(f"Server health error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/game-performance', methods=['GET'])
def game_performance():
    """Enhanced game performance metrics"""
    try:
        reviews_df = get_parquet_from_minio('reviews.parquet')
        
        if reviews_df is not None:
            # Top reviewed games (simulasi berdasarkan review_id patterns)
            top_games = {
                'Action Games': len(reviews_df[reviews_df['review_id'].str.contains('00[1-3]', na=False)]),
                'RPG Games': len(reviews_df[reviews_df['review_id'].str.contains('00[4-6]', na=False)]),
                'Strategy Games': len(reviews_df[reviews_df['review_id'].str.contains('00[7-9]', na=False)])
            }
            
            # Average ratings simulation
            avg_helpful_ratio = float(reviews_df['helpful_votes'].sum() / reviews_df['total_votes'].sum()) if reviews_df['total_votes'].sum() > 0 else 0
            
            # Recent trends (last 7 days simulation)
            recent_reviews = len(reviews_df.tail(100))  # Simulate recent activity
            
            # Calculate sentiment by game category (if advanced analysis available)
            if SENTIMENT_ANALYZER_AVAILABLE and sentiment_analyzer:
                try:
                    category_sentiment = {}
                    for category in top_games.keys():
                        category_sample = reviews_df[reviews_df['review_id'].str.contains(
                            '00[1-3]' if 'Action' in category else 
                            '00[4-6]' if 'RPG' in category else '00[7-9]', 
                            na=False
                        )].head(50)  # Small sample for performance
                        
                        if len(category_sample) > 0:
                            cat_sentiment = calculate_advanced_sentiment(category_sample)
                            category_sentiment[category] = cat_sentiment['sentiment_score']
                        else:
                            category_sentiment[category] = 50.0
                except Exception as e:
                    logger.warning(f"Category sentiment analysis failed: {e}")
                    category_sentiment = {}
            else:
                category_sentiment = {}
                
        else:
            top_games = {}
            avg_helpful_ratio = 0
            recent_reviews = 0
            category_sentiment = {}
            
        performance = {
            'top_games_by_reviews': top_games,
            'avg_helpful_ratio': round(avg_helpful_ratio * 100, 2),
            'recent_reviews_count': recent_reviews,
            'category_sentiment_scores': category_sentiment
        }
        
        return jsonify(performance)
    except Exception as e:
        logger.error(f"Game performance error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/sentiment/analyze', methods=['POST'])
def analyze_single_review():
    """Endpoint untuk menganalisis sentiment single review"""
    try:
        from flask import request
        
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
    """Enhanced health check dengan sentiment analyzer status"""
    status = {
        'status': 'healthy', 
        'service': 'Gaming Analytics API',
        'sentiment_analyzer': 'available' if SENTIMENT_ANALYZER_AVAILABLE else 'fallback_mode',
        'sentiment_method': 'advanced_nlp' if SENTIMENT_ANALYZER_AVAILABLE else 'basic'
    }
    
    if SENTIMENT_ANALYZER_AVAILABLE and sentiment_analyzer:
        # Test sentiment analyzer
        try:
            test_result = sentiment_analyzer.analyze_sentiment("This is a test review")
            status['sentiment_test'] = 'passed'
            status['sentiment_confidence'] = test_result.get('confidence', 0.0)
        except Exception as e:
            status['sentiment_test'] = f'failed: {str(e)}'
    
    return jsonify(status)

if __name__ == '__main__':
    logger.info("🚀 Starting Gaming Analytics API with Advanced Sentiment Analysis")
    logger.info(f"📊 Sentiment Analysis: {'Advanced NLP' if SENTIMENT_ANALYZER_AVAILABLE else 'Basic Fallback'}")
    app.run(debug=True, host='0.0.0.0', port=5000) 