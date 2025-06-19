#!/usr/bin/env python3
"""
Advanced Sentiment Analysis Module for Gaming Reviews
Menggunakan multiple NLP approaches untuk analisis sentiment yang lebih akurat
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Tuple, Optional

# NLP Libraries
try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False

try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    from nltk.stem import WordNetLemmatizer
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

try:
    from transformers import pipeline
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GameReviewSentimentAnalyzer:
    """
    Advanced sentiment analyzer khusus untuk gaming reviews
    Menggunakan multiple approaches dan gaming-specific vocabulary
    """
    
    def __init__(self, use_transformers=False):
        """
        Initialize sentiment analyzer
        
        Args:
            use_transformers: Whether to use transformer models (lebih akurat tapi lebih lambat)
        """
        self.use_transformers = use_transformers and TRANSFORMERS_AVAILABLE
        self.gaming_keywords = self._load_gaming_keywords()
        self.sentiment_adjustments = self._load_sentiment_adjustments()
        
        # Initialize analyzers
        self._init_analyzers()
        
    def _init_analyzers(self):
        """Initialize all available sentiment analyzers"""
        self.analyzers = {}
        
        # VADER Sentiment Analyzer
        if VADER_AVAILABLE:
            self.analyzers['vader'] = SentimentIntensityAnalyzer()
            logger.info("✅ VADER Sentiment Analyzer initialized")
        
        # TextBlob
        if TEXTBLOB_AVAILABLE:
            self.analyzers['textblob'] = True
            logger.info("✅ TextBlob Sentiment Analyzer initialized")
        
        # Transformer-based (optional)
        if self.use_transformers:
            try:
                self.analyzers['transformer'] = pipeline(
                    "sentiment-analysis",
                    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
                    return_all_scores=True
                )
                logger.info("✅ Transformer Sentiment Analyzer initialized")
            except Exception as e:
                logger.warning(f"⚠️ Could not initialize transformer model: {e}")
                self.use_transformers = False
        
        # Download NLTK data if needed
        if NLTK_AVAILABLE:
            try:
                nltk.download('punkt', quiet=True)
                nltk.download('stopwords', quiet=True)
                nltk.download('wordnet', quiet=True)
                nltk.download('vader_lexicon', quiet=True)
                self.lemmatizer = WordNetLemmatizer()
                self.stop_words = set(stopwords.words('english'))
                logger.info("✅ NLTK resources initialized")
            except Exception as e:
                logger.warning(f"⚠️ NLTK initialization error: {e}")
    
    def _load_gaming_keywords(self) -> Dict[str, float]:
        """Load gaming-specific sentiment keywords dengan weights yang lebih agresif"""
        gaming_positive = {
            'epic', 'amazing', 'addictive', 'fun', 'engaging', 'immersive',
            'masterpiece', 'brilliant', 'fantastic', 'gorgeous', 'stunning',
            'smooth', 'polished', 'innovative', 'creative', 'enjoyable',
            'excellent', 'outstanding', 'solid', 'rewarding', 'satisfying',
            'compelling', 'captivating', 'impressive', 'beautiful', 'perfect',
            'recommend', 'love', 'great', 'good', 'nice', 'wonderful'
        }
        
        gaming_negative = {
            'buggy', 'broken', 'unplayable', 'laggy', 'boring', 'repetitive',
            'terrible', 'awful', 'disappointing', 'frustrating', 'annoying',
            'glitchy', 'crashing', 'unfinished', 'overpriced', 'waste',
            'horrible', 'pathetic', 'useless', 'garbage', 'trash',
            'unoptimized', 'pay2win', 'microtransaction', 'cashgrab',
            'regret', 'hate', 'dislike', 'bad', 'worst', 'mess'
        }
        
        # Create sentiment weights dengan bobot yang lebih tinggi
        keywords = {}
        for word in gaming_positive:
            keywords[word] = 2.0  # Boost positive gaming terms lebih tinggi
        for word in gaming_negative:
            keywords[word] = -2.5  # Boost negative gaming terms lebih agresif
            
        return keywords
    
    def _load_sentiment_adjustments(self) -> Dict[str, float]:
        """Load context-based sentiment adjustments dengan deteksi kontradiksi"""
        return {
            'but': -0.8,      # "Good game but..." 
            'however': -0.8,  # "Great graphics however..."
            'except': -0.5,   # "Perfect except for..."
            'unfortunately': -1.0,
            'definitely': 0.4,
            'absolutely': 0.5,
            'highly': 0.4,
            'recommend': 0.6,
            'suggest': 0.3,   # "Would suggest" bisa positif/negatif
            'avoid': -1.2,
            'refund': -1.5,
            'waste': -1.3,
            'regret': -1.2,
            'buying': 0.0,    # Context dependent
            'overall': 0.2,   # Slight emphasis on overall opinion
        }
    
    def preprocess_text(self, text: str) -> str:
        """
        Advanced text preprocessing untuk gaming reviews
        """
        if not isinstance(text, str):
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove special gaming notation (e.g., "10/10", "9/10")
        text = re.sub(r'\d+/\d+', '', text)
        
        # Remove excessive punctuation but keep emotional indicators
        text = re.sub(r'[!]{2,}', '!', text)
        text = re.sub(r'[?]{2,}', '?', text)
        text = re.sub(r'[.]{3,}', '...', text)
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+', '', text)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def calculate_gaming_context_score(self, text: str) -> float:
        """
        Calculate sentiment adjustment based on gaming context
        """
        text_lower = text.lower()
        context_score = 0.0
        
        # Check for gaming-specific keywords
        for keyword, weight in self.gaming_keywords.items():
            if keyword in text_lower:
                context_score += weight
        
        # Check for sentiment modifiers
        for modifier, adjustment in self.sentiment_adjustments.items():
            if modifier in text_lower:
                context_score += adjustment
        
        # Normalize score
        return max(-2.0, min(2.0, context_score))
    
    def analyze_with_vader(self, text: str) -> Dict[str, float]:
        """Analyze sentiment using VADER"""
        if 'vader' not in self.analyzers:
            return {'compound': 0.0, 'pos': 0.0, 'neu': 0.0, 'neg': 0.0}
        
        scores = self.analyzers['vader'].polarity_scores(text)
        return scores
    
    def analyze_with_textblob(self, text: str) -> Dict[str, float]:
        """Analyze sentiment using TextBlob"""
        if 'textblob' not in self.analyzers:
            return {'polarity': 0.0, 'subjectivity': 0.0}
        
        blob = TextBlob(text)
        return {
            'polarity': blob.sentiment.polarity,
            'subjectivity': blob.sentiment.subjectivity
        }
    
    def analyze_with_transformer(self, text: str) -> Dict[str, float]:
        """Analyze sentiment using transformer model"""
        if 'transformer' not in self.analyzers:
            return {'positive': 0.33, 'neutral': 0.34, 'negative': 0.33}
        
        try:
            results = self.analyzers['transformer'](text)[0]
            scores = {}
            for result in results:
                label = result['label'].lower()
                if 'pos' in label:
                    scores['positive'] = result['score']
                elif 'neg' in label:
                    scores['negative'] = result['score']
                else:
                    scores['neutral'] = result['score']
            return scores
        except Exception as e:
            logger.warning(f"Transformer analysis failed: {e}")
            return {'positive': 0.33, 'neutral': 0.34, 'negative': 0.33}
    
    def analyze_sentiment(self, text: str) -> Dict[str, float]:
        """
        Comprehensive sentiment analysis menggunakan multiple methods
        
        Returns:
            Dict containing sentiment scores dan confidence metrics
        """
        if not text or not isinstance(text, str):
            return self._default_sentiment()
        
        # Preprocess text
        processed_text = self.preprocess_text(text)
        if not processed_text:
            return self._default_sentiment()
        
        # Get gaming context adjustment
        context_adjustment = self.calculate_gaming_context_score(processed_text)
        
        # Analyze with all available methods
        results = {}
        
        # VADER Analysis
        vader_scores = self.analyze_with_vader(processed_text)
        results['vader'] = vader_scores
        
        # TextBlob Analysis
        textblob_scores = self.analyze_with_textblob(processed_text)
        results['textblob'] = textblob_scores
        
        # Transformer Analysis (if enabled)
        if self.use_transformers:
            transformer_scores = self.analyze_with_transformer(processed_text)
            results['transformer'] = transformer_scores
        
        # Calculate ensemble score
        ensemble_score = self._calculate_ensemble_score(results, context_adjustment)
        
        return {
            'overall_sentiment': ensemble_score['sentiment'],
            'confidence': ensemble_score['confidence'],
            'positive_score': ensemble_score['positive'],
            'neutral_score': ensemble_score['neutral'],
            'negative_score': ensemble_score['negative'],
            'gaming_context_adjustment': context_adjustment,
            'detailed_scores': results
        }
    
    def _calculate_ensemble_score(self, results: Dict, context_adj: float) -> Dict[str, float]:
        """Calculate weighted ensemble score dengan improved sensitivity"""
        positive_scores = []
        negative_scores = []
        neutral_scores = []
        
        # Extract scores from VADER (weighted lebih tinggi untuk gaming reviews)
        if 'vader' in results:
            vader = results['vader']
            # VADER umumnya bagus untuk informal text seperti gaming reviews
            positive_scores.append(vader['pos'] * 1.2)  # Boost VADER weight
            negative_scores.append(vader['neg'] * 1.2)
            neutral_scores.append(vader['neu'])
        
        # Extract scores from TextBlob
        if 'textblob' in results:
            tb = results['textblob']
            polarity = tb['polarity']
            if polarity > 0.1:
                positive_scores.append((polarity + 1) / 2)
                negative_scores.append(0.0)
                neutral_scores.append(1 - (polarity + 1) / 2)
            elif polarity < -0.1:
                positive_scores.append(0.0)
                negative_scores.append(abs(polarity + 1) / 2)
                neutral_scores.append(1 - abs(polarity + 1) / 2)
            else:
                positive_scores.append(0.2)
                negative_scores.append(0.2)
                neutral_scores.append(0.6)
        
        # Extract scores from Transformer
        if 'transformer' in results:
            trans = results['transformer']
            positive_scores.append(trans.get('positive', 0.33))
            negative_scores.append(trans.get('negative', 0.33))
            neutral_scores.append(trans.get('neutral', 0.33))
        
        # Calculate weighted averages
        pos_avg = np.mean(positive_scores) if positive_scores else 0.33
        neg_avg = np.mean(negative_scores) if negative_scores else 0.33
        neu_avg = np.mean(neutral_scores) if neutral_scores else 0.33
        
        # Apply gaming context adjustment dengan weight yang lebih besar
        if context_adj > 0:
            boost_factor = min(abs(context_adj) * 0.15, 0.4)  # Max 40% boost
            pos_avg = min(1.0, pos_avg + boost_factor)
            neg_avg = max(0.0, neg_avg - boost_factor * 0.5)
        elif context_adj < 0:
            boost_factor = min(abs(context_adj) * 0.15, 0.4)  # Max 40% boost
            neg_avg = min(1.0, neg_avg + boost_factor)
            pos_avg = max(0.0, pos_avg - boost_factor * 0.5)
        
        # Normalize scores
        total = pos_avg + neg_avg + neu_avg
        if total > 0:
            pos_avg /= total
            neg_avg /= total
            neu_avg /= total
        
        # Improved sentiment determination dengan threshold yang lebih sensitif
        threshold = 0.35  # Lower threshold untuk classification
        
        if neg_avg > threshold and neg_avg > pos_avg:
            sentiment = 'negative'
            confidence = neg_avg
        elif pos_avg > threshold and pos_avg > neg_avg:
            sentiment = 'positive'
            confidence = pos_avg
        else:
            sentiment = 'neutral'
            confidence = max(neu_avg, 0.4)  # Minimum confidence untuk neutral
        
        # Special case: Jika VADER compound sangat negatif, override
        if 'vader' in results and results['vader']['compound'] < -0.5:
            if sentiment != 'negative':
                sentiment = 'negative'
                confidence = min(0.8, confidence + 0.2)  # Boost confidence
                neg_avg = max(neg_avg, 0.6)  # Ensure high negative score
        
        # Special case: Jika VADER compound sangat positif, override
        elif 'vader' in results and results['vader']['compound'] > 0.5:
            if sentiment != 'positive':
                sentiment = 'positive'
                confidence = min(0.8, confidence + 0.2)  # Boost confidence
                pos_avg = max(pos_avg, 0.6)  # Ensure high positive score
        
        return {
            'sentiment': sentiment,
            'confidence': float(confidence),
            'positive': float(pos_avg),
            'negative': float(neg_avg),
            'neutral': float(neu_avg)
        }
    
    def _default_sentiment(self) -> Dict[str, float]:
        """Return default neutral sentiment"""
        return {
            'overall_sentiment': 'neutral',
            'confidence': 0.5,
            'positive_score': 0.33,
            'neutral_score': 0.34,
            'negative_score': 0.33,
            'gaming_context_adjustment': 0.0,
            'detailed_scores': {}
        }
    
    def analyze_batch(self, texts: List[str], batch_size: int = 100) -> List[Dict[str, float]]:
        """
        Analyze multiple texts efficiently in batches
        """
        results = []
        total = len(texts)
        
        for i in range(0, total, batch_size):
            batch = texts[i:i + batch_size]
            batch_results = []
            
            for text in batch:
                result = self.analyze_sentiment(text)
                batch_results.append(result)
            
            results.extend(batch_results)
            
            if i % (batch_size * 10) == 0:
                logger.info(f"Processed {min(i + batch_size, total)}/{total} texts")
        
        return results
    
    def get_summary_statistics(self, sentiment_results: List[Dict[str, float]]) -> Dict[str, float]:
        """
        Calculate summary statistics from sentiment analysis results
        """
        if not sentiment_results:
            return {
                'total_reviews': 0,
                'positive_percentage': 0.0,
                'negative_percentage': 0.0,
                'neutral_percentage': 0.0,
                'average_confidence': 0.0,
                'sentiment_score': 50.0  # Neutral score for dashboard
            }
        
        total = len(sentiment_results)
        positive_count = sum(1 for r in sentiment_results if r['overall_sentiment'] == 'positive')
        negative_count = sum(1 for r in sentiment_results if r['overall_sentiment'] == 'negative')
        neutral_count = sum(1 for r in sentiment_results if r['overall_sentiment'] == 'neutral')
        
        avg_confidence = np.mean([r['confidence'] for r in sentiment_results])
        
        positive_pct = (positive_count / total) * 100
        negative_pct = (negative_count / total) * 100
        neutral_pct = (neutral_count / total) * 100
        
        # Calculate overall sentiment score (0-100, where 50 is neutral)
        sentiment_score = 50 + (positive_pct - negative_pct) / 2
        
        return {
            'total_reviews': total,
            'positive_percentage': round(positive_pct, 2),
            'negative_percentage': round(negative_pct, 2),
            'neutral_percentage': round(neutral_pct, 2),
            'average_confidence': round(avg_confidence, 3),
            'sentiment_score': round(sentiment_score, 2)
        }

# Utility functions for easy usage
def quick_sentiment_analysis(text: str, use_transformers: bool = False) -> str:
    """Quick sentiment analysis returning just the sentiment label"""
    analyzer = GameReviewSentimentAnalyzer(use_transformers=use_transformers)
    result = analyzer.analyze_sentiment(text)
    return result['overall_sentiment']

def analyze_review_dataframe(df: pd.DataFrame, text_column: str, use_transformers: bool = False) -> pd.DataFrame:
    """
    Analyze sentiment for all reviews in a DataFrame
    
    Args:
        df: DataFrame containing reviews
        text_column: Name of column containing review text
        use_transformers: Whether to use transformer models
    
    Returns:
        DataFrame with added sentiment columns
    """
    analyzer = GameReviewSentimentAnalyzer(use_transformers=use_transformers)
    
    # Extract texts
    texts = df[text_column].fillna('').astype(str).tolist()
    
    # Analyze sentiments
    logger.info(f"Analyzing sentiment for {len(texts)} reviews...")
    sentiment_results = analyzer.analyze_batch(texts)
    
    # Add results to DataFrame
    df_copy = df.copy()
    df_copy['sentiment'] = [r['overall_sentiment'] for r in sentiment_results]
    df_copy['sentiment_confidence'] = [r['confidence'] for r in sentiment_results]
    df_copy['positive_score'] = [r['positive_score'] for r in sentiment_results]
    df_copy['negative_score'] = [r['negative_score'] for r in sentiment_results]
    df_copy['neutral_score'] = [r['neutral_score'] for r in sentiment_results]
    
    return df_copy

if __name__ == "__main__":
    # Test the sentiment analyzer
    test_reviews = [
        "This game is absolutely amazing! The graphics are stunning and gameplay is so addictive.",
        "Buggy mess. Crashes every 10 minutes. Complete waste of money.",
        "Good game but could use some improvements. Graphics are nice however the story is boring.",
        "Meh, it's okay I guess. Nothing special but not terrible either."
    ]
    
    analyzer = GameReviewSentimentAnalyzer(use_transformers=False)
    
    print("🧪 Testing Advanced Sentiment Analysis:")
    print("=" * 60)
    
    for i, review in enumerate(test_reviews, 1):
        result = analyzer.analyze_sentiment(review)
        print(f"\nReview {i}: {review[:50]}...")
        print(f"Sentiment: {result['overall_sentiment']} (confidence: {result['confidence']:.3f})")
        print(f"Scores - Pos: {result['positive_score']:.3f}, Neg: {result['negative_score']:.3f}, Neu: {result['neutral_score']:.3f}")
        print(f"Gaming Context Adjustment: {result['gaming_context_adjustment']:.3f}")
    
    # Summary statistics
    stats = analyzer.get_summary_statistics([analyzer.analyze_sentiment(review) for review in test_reviews])
    print(f"\n📊 Summary Statistics:")
    print(f"Overall Sentiment Score: {stats['sentiment_score']:.1f}/100")
    print(f"Positive: {stats['positive_percentage']:.1f}%")
    print(f"Negative: {stats['negative_percentage']:.1f}%")
    print(f"Neutral: {stats['neutral_percentage']:.1f}%") 