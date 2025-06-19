import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time

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

def check_api_health():
    """Check if API is healthy"""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

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

def main():
    st.title("🎮 Gaming Analytics Dashboard")
    st.markdown("Real-time insights dari Steam gaming data lakehouse")
    
    # Check API health first
    api_healthy = check_api_health()
    
    # Sidebar status
    st.sidebar.subheader("🔄 System Status")
    if api_healthy:
        st.sidebar.success("✅ Analytics API: Online")
        
        # Auto-refresh option
        auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=False)
        refresh_button = st.sidebar.button("🔄 Refresh Now")
        
        # Display dashboard
        display_dashboard()
        
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

def display_dashboard():
    """Display the main dashboard"""
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
        
        # Overview metrics
        st.subheader("📊 Overview Metrics")
        if overview_data:
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
                    delta=None
                )
            
            with col3:
                st.metric(
                    label="Log Entries",
                    value=f"{overview_data.get('total_log_entries', 0):,}",
                    delta=None
                )
            
            with col4:
                st.metric(
                    label="Avg Playtime",
                    value=f"{overview_data.get('avg_playtime', 0):.1f}h",
                    delta=None
                )
        else:
            st.error("❌ Could not load overview data")
        
        # Player Analytics
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
                st.metric(
                    label="Active Players",
                    value=f"{player_data.get('active_players', 0):,}"
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
                
                # Additional metrics for advanced analysis
                if sentiment_method == 'advanced_nlp':
                    st.info(f"🧠 Advanced NLP Analysis | {player_data.get('total_analyzed_reviews', 0)} reviews analyzed")
                    if player_data.get('high_confidence_count', 0) > 0:
                        st.info(f"🎯 {player_data.get('high_confidence_count', 0)} high-confidence predictions")
                
            else:
                st.warning("Could not load player metrics")
        
        # Server Health
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
                st.metric("Active Servers", f"{server_data.get('active_servers', 0)}")
                st.metric("Recent Errors", f"{server_data.get('recent_errors', 0)}")
                st.metric("Total Connections", f"{server_data.get('total_connections', 0):,}")
            else:
                st.warning("Could not load server health data")
        
        # Game Performance
        st.subheader("🎯 Game Performance")
        if game_data and 'top_games_by_reviews' in game_data:
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
            else:
                st.info("No game performance data available")
        else:
            st.warning("Could not load game performance data")
        
        # Update timestamp
        st.sidebar.info(f"Last Updated: {pd.Timestamp.now().strftime('%H:%M:%S')}")
        
    except Exception as e:
        st.error(f"🔥 Dashboard Error: {str(e)}")
        st.info("Try refreshing the page or check the API logs")

if __name__ == "__main__":
    main()