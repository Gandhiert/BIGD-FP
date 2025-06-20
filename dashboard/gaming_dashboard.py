import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime, timedelta

# Config
API_BASE_URL = "http://localhost:5000/api"

st.set_page_config(
    page_title="Gaming Analytics Dashboard",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

def fetch_data(endpoint, timeout=10):
    """Fetch data from API endpoint with better error handling"""
    try:
        with st.spinner(f'Loading {endpoint}...'):
            response = requests.get(f"{API_BASE_URL}/{endpoint}", timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                st.error(f"API Error {response.status_code} for {endpoint}")
                return None
    except requests.exceptions.ConnectionError:
        st.error(f"❌ Cannot connect to API at {API_BASE_URL}")
        return None
    except requests.exceptions.Timeout:
        st.error(f"⏱️ Timeout connecting to API for {endpoint}")
        return None
    except Exception as e:
        st.error(f"🔥 Error fetching {endpoint}: {str(e)}")
        return None

def fetch_streaming_data(endpoint, params=None, timeout=10):
    """Fetch streaming data with parameters"""
    try:
        url = f"{API_BASE_URL}/streaming/{endpoint}"
        with st.spinner(f'Loading streaming {endpoint}...'):
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                st.error(f"Streaming API Error {response.status_code} for {endpoint}")
                return None
    except requests.exceptions.ConnectionError:
        st.error(f"❌ Cannot connect to streaming API")
        return None
    except requests.exceptions.Timeout:
        st.error(f"⏱️ Timeout connecting to streaming API for {endpoint}")
        return None
    except Exception as e:
        st.error(f"🔥 Error fetching streaming {endpoint}: {str(e)}")
        return None

def check_api_health():
    """Check if API is healthy"""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

def check_streaming_status():
    """Check streaming data availability"""
    try:
        response = requests.get(f"{API_BASE_URL}/streaming/status", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def show_mock_data():
    """Show mock data when API is not available"""
    st.warning("🔌 API not available. Showing mock data for demo purposes.")
    
    # Mock overview data
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Games", "5,000", "↗️ 150")
    with col2:
        st.metric("Total Reviews", "10,000", "↗️ 500")
    with col3:
        st.metric("Log Entries", "50,000", "↗️ 2,000")
    with col4:
        st.metric("Avg Playtime", "45.2h", "↗️ 2.1h")
    
    # Mock charts
    st.subheader("📊 Demo Data")
    
    # Mock playtime distribution
    mock_playtime = pd.DataFrame({
        'Playtime Range': ['0-10h', '10-50h', '50-100h', '100h+'],
        'Players': [2500, 4000, 2000, 1500]
    })
    
    fig = px.pie(mock_playtime, values='Players', names='Playtime Range', 
                 title="Player Playtime Distribution (Mock Data)")
    st.plotly_chart(fig, use_container_width=True)
    
    st.info("💡 Start the Analytics API to see real data from your data lakehouse!")

def display_streaming_status():
    """Display streaming data status in sidebar"""
    streaming_status = check_streaming_status()
    
    st.sidebar.subheader("📡 Streaming Status")
    if streaming_status and streaming_status.get('streaming_bucket_available'):
        st.sidebar.success("✅ Streaming Data: Online")
        
        # Show data type availability
        data_types = streaming_status.get('data_types', {})
        for data_type, info in data_types.items():
            if info.get('available'):
                st.sidebar.text(f"✅ {data_type.replace('_', ' ').title()}: {info.get('file_count', 0)} files")
            else:
                st.sidebar.text(f"❌ {data_type.replace('_', ' ').title()}: No data")
        
        # Show last activity
        if streaming_status.get('last_activity'):
            last_activity = datetime.fromisoformat(streaming_status['last_activity'].replace('Z', '+00:00'))
            time_ago = datetime.now() - last_activity.replace(tzinfo=None)
            st.sidebar.info(f"Last activity: {int(time_ago.total_seconds() / 60)}m ago")
            
    else:
        st.sidebar.warning("⚠️ Streaming Data: Limited")
        st.sidebar.text("Using warehouse data only")

def main():
    st.title("🎮 Gaming Analytics Dashboard")
    st.markdown("Real-time insights dari Steam gaming data lakehouse")
    
    # Check API health first
    api_healthy = check_api_health()
    
    # Sidebar status
    st.sidebar.subheader("🔄 System Status")
    if api_healthy:
        st.sidebar.success("✅ Analytics API: Online")
        
        # Display streaming status
        display_streaming_status()
        
        # Auto-refresh option
        auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=False)
        refresh_button = st.sidebar.button("🔄 Refresh Now")
        
        # Tab selection
        tab1, tab2, tab3 = st.tabs(["📊 Main Dashboard", "📡 Real-time Streaming", "⚙️ System Monitor"])
        
        with tab1:
            display_dashboard()
        
        with tab2:
            display_streaming_dashboard()
            
        with tab3:
            display_system_monitor()
        
        # Handle auto-refresh without infinite loop
        if auto_refresh:
            # Use session state to track refresh
            if 'last_refresh' not in st.session_state:
                st.session_state.last_refresh = time.time()
            
            current_time = time.time()
            if current_time - st.session_state.last_refresh >= 30:
                st.session_state.last_refresh = current_time
                st.rerun()
            
            # Show countdown
            time_since_refresh = current_time - st.session_state.last_refresh
            next_refresh = 30 - int(time_since_refresh)
            st.sidebar.info(f"Next refresh in: {max(0, next_refresh)}s")
            
    else:
        st.sidebar.error("❌ Analytics API: Offline")
        st.sidebar.markdown("""
        **Troubleshooting:**
        1. Check if API is running on port 5000
        2. Run: `python data_lake/scripts/analytics_api.py`
        3. Ensure MinIO is running with data
        """)
        
        # Show mock data
        show_mock_data()

def display_streaming_dashboard():
    """Display real-time streaming data dashboard"""
    st.subheader("📡 Real-time Streaming Dashboard")
    st.markdown("Live data dari Kafka streaming pipeline")
    
    # Streaming controls
    col1, col2, col3 = st.columns(3)
    with col1:
        hours_back = st.selectbox("Time Range", [1, 6, 12, 24], index=0, key="streaming_hours")
    with col2:
        limit = st.selectbox("Max Records", [50, 100, 200, 500], index=1, key="streaming_limit")
    with col3:
        if st.button("🔄 Refresh Streaming Data"):
            st.rerun()
    
    # Real-time Player Events
    st.subheader("👥 Live Player Events")
    player_events = fetch_streaming_data("player-events", {"hours": hours_back, "limit": limit})
    
    if player_events and player_events.get('events'):
        col1, col2 = st.columns([2, 1])
        
        with col1:
            events_df = pd.DataFrame(player_events['events'])
            st.dataframe(events_df.head(20), use_container_width=True)
        
        with col2:
            st.metric("Total Events", f"{player_events.get('total_count', 0):,}")
            st.metric("Sample Size", f"{player_events.get('sample_size', 0):,}")
            
            # Event type distribution
            if 'event_type' in events_df.columns:
                event_counts = events_df['event_type'].value_counts()
                fig_events = px.pie(
                    values=event_counts.values,
                    names=event_counts.index,
                    title="Event Types"
                )
                st.plotly_chart(fig_events, use_container_width=True)
    else:
        st.info("No player events data available")
    
    # Real-time Game Metrics
    st.subheader("🎯 Live Game Metrics")
    game_metrics = fetch_streaming_data("game-metrics", {"hours": hours_back, "limit": limit})
    
    if game_metrics and game_metrics.get('metrics'):
        summary = game_metrics.get('summary', {})
        
        # Metrics overview
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Avg Concurrent Players", f"{summary.get('avg_concurrent_players', 0):.0f}")
        with col2:
            st.metric("Max Concurrent Players", f"{summary.get('max_concurrent_players', 0):,}")
        with col3:
            st.metric("Avg Crash Rate", f"{summary.get('avg_crash_rate', 0):.1f}%")
        with col4:
            st.metric("Max Crash Rate", f"{summary.get('max_crash_rate', 0):.1f}%")
        
        # Health status distribution
        if 'health_distribution' in summary:
            health_data = summary['health_distribution']
            health_df = pd.DataFrame(
                list(health_data.items()),
                columns=['Status', 'Count']
            )
            
            fig_health = px.bar(
                health_df,
                x='Status',
                y='Count',
                title="Server Health Distribution",
                color='Status',
                color_discrete_map={
                    'good': '#28a745',
                    'moderate': '#ffc107',
                    'poor': '#dc3545'
                }
            )
            st.plotly_chart(fig_health, use_container_width=True)
        
        # Detailed metrics table
        metrics_df = pd.DataFrame(game_metrics['metrics'])
        st.dataframe(metrics_df.head(10), use_container_width=True)
        
    else:
        st.info("No game metrics data available")
    
    # Real-time Reviews
    st.subheader("📝 Live Reviews")
    streaming_reviews = fetch_streaming_data("reviews", {"hours": hours_back, "limit": limit})
    
    if streaming_reviews and streaming_reviews.get('reviews'):
        stats = streaming_reviews.get('statistics', {})
        
        # Review statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Reviews", f"{streaming_reviews.get('total_count', 0):,}")
        with col2:
            avg_rating = stats.get('avg_rating', 0)
            st.metric("Avg Rating", f"{avg_rating:.1f}/5" if avg_rating > 0 else "N/A")
        with col3:
            st.metric("Total Helpful Votes", f"{stats.get('total_helpful_votes', 0):,}")
        
        # Sentiment distribution (if available)
        if 'sentiment_distribution' in stats:
            sentiment_data = stats['sentiment_distribution']
            sentiment_df = pd.DataFrame(
                list(sentiment_data.items()),
                columns=['Sentiment', 'Count']
            )
            
            fig_sentiment = px.pie(
                sentiment_df,
                values='Count',
                names='Sentiment',
                title="Real-time Sentiment Distribution"
            )
            st.plotly_chart(fig_sentiment, use_container_width=True)
        
        # Recent reviews table
        reviews_df = pd.DataFrame(streaming_reviews['reviews'])
        if len(reviews_df) > 0:
            # Show only relevant columns
            display_cols = ['processed_at', 'rating', 'helpful_votes', 'sentiment_indicator']
            available_cols = [col for col in display_cols if col in reviews_df.columns]
            if available_cols:
                st.dataframe(reviews_df[available_cols].head(10), use_container_width=True)
            else:
                st.dataframe(reviews_df.head(10), use_container_width=True)
        
    else:
        st.info("No streaming reviews data available")

def display_system_monitor():
    """Display system monitoring and configuration"""
    st.subheader("⚙️ System Monitor & Configuration")
    
    # API Health Check
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔧 API Configuration")
        
        # Get performance config
        perf_config = fetch_data("config/performance")
        if perf_config:
            st.json(perf_config)
            
            # Configuration controls
            if st.button("Clear Sentiment Cache"):
                try:
                    response = requests.post(f"{API_BASE_URL}/config/cache/clear")
                    if response.status_code == 200:
                        st.success("✅ Cache cleared successfully")
                    else:
                        st.error("❌ Failed to clear cache")
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.warning("Could not load performance configuration")
    
    with col2:
        st.subheader("📊 Streaming Status Details")
        
        # Detailed streaming status
        streaming_status = check_streaming_status()
        if streaming_status:
            st.json(streaming_status)
        else:
            st.warning("Could not load streaming status")
    
    # System Metrics
    st.subheader("📈 System Performance")
    
    # Get health data
    health_data = fetch_data("health")
    if health_data:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status = health_data.get('status', 'unknown')
            st.metric("API Status", status.title())
        
        with col2:
            sentiment_status = health_data.get('sentiment_analyzer', 'unknown')
            st.metric("Sentiment Analyzer", sentiment_status.title())
        
        with col3:
            cache_entries = health_data.get('cache_entries', 0)
            st.metric("Cache Entries", f"{cache_entries}")
        
        # Show full health data
        st.json(health_data)
    else:
        st.warning("Could not load system health data")

def display_dashboard():
    """Display the main dashboard with real-time indicators"""
    try:
        # Show loading message
        status_placeholder = st.empty()
        status_placeholder.info("🔄 Loading dashboard data...")
        
        # Fetch all data with error handling
        overview_data = fetch_data("dashboard/overview")
        player_data = fetch_data("dashboard/player-metrics") 
        server_data = fetch_data("dashboard/server-health")
        game_data = fetch_data("dashboard/game-performance")
        
        # Clear loading message
        status_placeholder.empty()
        
        # Overview metrics with real-time indicators
        st.subheader("📊 Overview Metrics")
        if overview_data:
            # Show real-time indicators
            streaming_available = overview_data.get('streaming_data_available', {})
            real_time_players = overview_data.get('real_time_active_players', 0)
            
            if any(streaming_available.values()):
                st.success("🔴 LIVE: Real-time data streaming active")
            else:
                st.info("📦 Using warehouse data only")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    label="Total Games",
                    value=f"{overview_data.get('total_games', 0):,}",
                    delta=None
                )
            
            with col2:
                st.metric(
                    label="Total Reviews", 
                    value=f"{overview_data.get('total_reviews', 0):,}",
                    delta="🔴 Live" if streaming_available.get('reviews') else None
                )
            
            with col3:
                st.metric(
                    label="Log Entries",
                    value=f"{overview_data.get('total_log_entries', 0):,}",
                    delta="🔴 Live" if streaming_available.get('logs') else None
                )
            
            with col4:
                st.metric(
                    label="Active Players",
                    value=f"{real_time_players:,}",
                    delta="🔴 Real-time" if real_time_players > 0 else None
                )
        else:
            st.error("❌ Could not load overview data")
        
        # Player Analytics with enhanced real-time features
        st.subheader("👥 Player Analytics")
        col1, col2 = st.columns(2)
        
        with col1:
            if player_data and 'playtime_distribution' in player_data:
                playtime_df = pd.DataFrame(
                    list(player_data['playtime_distribution'].items()),
                    columns=['Playtime Range', 'Players']
                )
                
                if not playtime_df.empty:
                    fig_playtime = px.pie(
                        playtime_df, 
                        values='Players', 
                        names='Playtime Range',
                        title="Player Playtime Distribution"
                    )
                    st.plotly_chart(fig_playtime, use_container_width=True)
                else:
                    st.info("No playtime data available")
            else:
                st.warning("Could not load player playtime data")
        
        with col2:
            if player_data:
                # Real-time active players
                real_time_active = player_data.get('real_time_active_players', 0)
                st.metric(
                    label="🔴 Real-time Active Players",
                    value=f"{real_time_active:,}",
                    help="Players active in the last hour from streaming data"
                )
                
                # Enhanced sentiment display
                sentiment_score = player_data.get('sentiment_score', 50.0)
                sentiment_method = player_data.get('sentiment_method', 'unknown')
                confidence = player_data.get('sentiment_confidence', 0.0)
                
                # Color-coded sentiment score
                if sentiment_score >= 70:
                    sentiment_color = "🟢"
                elif sentiment_score >= 50:
                    sentiment_color = "🟡"
                else:
                    sentiment_color = "🔴"
                
                st.metric(
                    label=f"Sentiment Score {sentiment_color}",
                    value=f"{sentiment_score:.1f}/100",
                    help=f"Method: {sentiment_method.replace('_', ' ').title()}, Confidence: {confidence:.2f}"
                )
                
                # Sentiment breakdown (if available)
                if 'sentiment_breakdown' in player_data:
                    breakdown = player_data['sentiment_breakdown']
                    st.write("**Sentiment Breakdown:**")
                    col_pos, col_neu, col_neg = st.columns(3)
                    
                    with col_pos:
                        st.metric(
                            label="Positive",
                            value=f"{breakdown.get('positive', 0):.1f}%",
                            delta=None
                        )
                    with col_neu:
                        st.metric(
                            label="Neutral", 
                            value=f"{breakdown.get('neutral', 0):.1f}%",
                            delta=None
                        )
                    with col_neg:
                        st.metric(
                            label="Negative",
                            value=f"{breakdown.get('negative', 0):.1f}%",
                            delta=None
                        )
                
                # Show data sources info
                data_sources = player_data.get('data_sources', {})
                st.info(f"📊 Data: {data_sources.get('reviews_count', 0)} reviews, "
                       f"{data_sources.get('player_events_count', 0)} events, "
                       f"{data_sources.get('logs_count', 0)} logs")
                
                # Additional metrics for advanced analysis
                if sentiment_method == 'advanced_nlp':
                    st.info(f"🧠 Advanced NLP Analysis | {player_data.get('total_analyzed_reviews', 0)} reviews analyzed")
                    if player_data.get('high_confidence_count', 0) > 0:
                        st.info(f"🎯 {player_data.get('high_confidence_count', 0)} high-confidence predictions")
                
            else:
                st.warning("Could not load player metrics")
        
        # Server Health with real-time alerts
        st.subheader("🖥️ Server Health & Performance")
        col1, col2 = st.columns(2)
        
        with col1:
            if server_data and 'log_levels' in server_data and server_data['log_levels']:
                log_df = pd.DataFrame(
                    list(server_data['log_levels'].items()),
                    columns=['Log Level', 'Count']
                )
                
                fig_logs = px.bar(
                    log_df,
                    x='Log Level',
                    y='Count',
                    title="Log Level Distribution",
                    color='Log Level',
                    color_discrete_map={
                        'ERROR': '#ff4444',
                        'WARNING': '#ffaa00', 
                        'INFO': '#44ff44',
                        'DEBUG': '#4444ff'
                    }
                )
                st.plotly_chart(fig_logs, use_container_width=True)
            else:
                st.info("No log data available")
        
        with col2:
            if server_data:
                # Server performance metrics
                server_perf = server_data.get('server_performance', {})
                
                st.metric("🔴 Concurrent Players", f"{server_perf.get('concurrent_players', 0):,}")
                st.metric("Recent Errors", f"{server_data.get('recent_errors', 0)}")
                st.metric("Crash Rate", f"{server_perf.get('crash_rate', 0):.1f}%")
                
                # Health status indicator
                health_status = server_perf.get('health_status', 'unknown')
                if health_status == 'good':
                    st.success(f"✅ Server Health: {health_status.title()}")
                elif health_status == 'moderate':
                    st.warning(f"⚠️ Server Health: {health_status.title()}")
                elif health_status == 'poor':
                    st.error(f"❌ Server Health: {health_status.title()}")
                else:
                    st.info(f"❓ Server Health: {health_status.title()}")
                
                # Real-time alerts
                alerts = server_data.get('real_time_alerts', [])
                if alerts:
                    st.subheader("🚨 Active Alerts")
                    for alert in alerts:
                        if alert['severity'] == 'high':
                            st.error(f"🚨 {alert['message']}")
                        else:
                            st.warning(f"⚠️ {alert['message']}")
                            
            else:
                st.warning("Could not load server health data")
        
        # Game Performance with real-time activity
        st.subheader("🎯 Game Performance")
        if game_data:
            # Real-time performance data
            real_time_perf = game_data.get('real_time_performance', {})
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🔴 Active Games", f"{real_time_perf.get('total_active_games', 0)}")
            with col2:
                st.metric("🔴 Concurrent Players", f"{real_time_perf.get('concurrent_players', 0):,}")
            with col3:
                st.metric("Avg Crash Rate", f"{real_time_perf.get('avg_crash_rate', 0):.1f}%")
            with col4:
                st.metric("Poor Health Games", f"{real_time_perf.get('games_with_poor_health', 0)}")
            
            # Game categories chart
            if 'top_games_by_reviews' in game_data:
                games_df = pd.DataFrame(
                    list(game_data['top_games_by_reviews'].items()),
                    columns=['Game Category', 'Reviews']
                )
                
                if not games_df.empty:
                    fig_games = px.bar(
                        games_df,
                        x='Game Category',
                        y='Reviews',
                        title="Top Game Categories by Reviews"
                    )
                    st.plotly_chart(fig_games, use_container_width=True)
            
            # Player engagement from streaming
            engagement = game_data.get('player_engagement', {})
            if engagement:
                st.subheader("🎮 Real-time Player Engagement")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Game Sessions", f"{engagement.get('total_sessions', 0):,}")
                with col2:
                    st.metric("Achievements", f"{engagement.get('achievement_unlocks', 0):,}")
                with col3:
                    st.metric("Purchases", f"{engagement.get('purchases', 0):,}")
                    
        else:
            st.warning("Could not load game performance data")
        
        # Update timestamp with data source info
        if overview_data and overview_data.get('last_updated'):
            last_updated = overview_data['last_updated']
            st.sidebar.success(f"🔄 Last Updated: {pd.Timestamp(last_updated).strftime('%H:%M:%S')}")
        else:
            st.sidebar.info(f"🔄 Last Updated: {pd.Timestamp.now().strftime('%H:%M:%S')}")
        
    except Exception as e:
        st.error(f"🔥 Dashboard Error: {str(e)}")
        st.info("Try refreshing the page or check the API logs")

if __name__ == "__main__":
    main()