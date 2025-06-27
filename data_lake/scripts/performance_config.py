#!/usr/bin/env python3
"""
Performance Configuration Utility for Analytics API
Script untuk mengatur mode performance dan optimasi resource
"""

import requests
import json
import argparse
import sys

API_BASE_URL = "http://localhost:5000"

def check_api_status():
    """Check if Analytics API is running"""
    try:
        response = requests.get(f"{API_BASE_URL}/api/health", timeout=5)
        return response.status_code == 200, response.json()
    except requests.exceptions.RequestException as e:
        return False, str(e)

def get_performance_config():
    """Get current performance configuration"""
    try:
        response = requests.get(f"{API_BASE_URL}/api/config/performance", timeout=5)
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, f"HTTP {response.status_code}: {response.text}"
    except requests.exceptions.RequestException as e:
        return False, str(e)

def set_lightweight_mode(enable=True):
    """Enable or disable lightweight mode"""
    try:
        payload = {"lightweight_mode": enable}
        response = requests.post(
            f"{API_BASE_URL}/api/config/performance",
            json=payload,
            timeout=5
        )
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, f"HTTP {response.status_code}: {response.text}"
    except requests.exceptions.RequestException as e:
        return False, str(e)

def clear_cache():
    """Clear sentiment analysis cache"""
    try:
        response = requests.post(f"{API_BASE_URL}/api/config/cache/clear", timeout=5)
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, f"HTTP {response.status_code}: {response.text}"
    except requests.exceptions.RequestException as e:
        return False, str(e)

def print_status():
    """Print current API and performance status"""
    print("🔍 Checking Analytics API status...")
    
    # Check API health
    api_running, health_data = check_api_status()
    if not api_running:
        print(f"❌ Analytics API is not running: {health_data}")
        return False
    
    print("✅ Analytics API is running")
    print(f"   Sentiment Analyzer: {health_data.get('sentiment_analyzer', 'unknown')}")
    print(f"   Lightweight Mode: {health_data.get('lightweight_mode', 'unknown')}")
    print(f"   Cache Entries: {health_data.get('cache_entries', 'unknown')}")
    
    # Get detailed performance config
    config_success, config_data = get_performance_config()
    if config_success:
        print("\n📊 Performance Configuration:")
        print(f"   Lightweight Mode: {'🟢 Enabled' if config_data.get('lightweight_mode') else '🔴 Disabled'}")
        print(f"   Sentiment Analyzer: {'🟢 Available' if config_data.get('sentiment_analyzer_available') else '🔴 Disabled'}")
        print(f"   Cache Size: {config_data.get('cache_size', 0)}/{config_data.get('max_cache_size', 10)}")
        print(f"   Sample Size: {config_data.get('sample_size', 'unknown')} reviews")
        print(f"   Use Transformers: {'🟢 Yes' if config_data.get('use_transformers') else '🔴 No'}")
    else:
        print(f"⚠️ Could not get performance config: {config_data}")
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Analytics API Performance Configuration")
    parser.add_argument('--status', action='store_true', help='Show current status')
    parser.add_argument('--lightweight', action='store_true', help='Enable lightweight mode')
    parser.add_argument('--full-mode', action='store_true', help='Disable lightweight mode')
    parser.add_argument('--clear-cache', action='store_true', help='Clear sentiment analysis cache')
    
    args = parser.parse_args()
    
    if not any([args.status, args.lightweight, args.full_mode, args.clear_cache]):
        parser.print_help()
        return
    
    if args.status:
        print_status()
    
    if args.lightweight:
        print("\n🚀 Enabling lightweight mode...")
        success, result = set_lightweight_mode(True)
        if success:
            print("✅ Lightweight mode enabled")
            print("   - NLP processing disabled")
            print("   - Faster response times")
            print("   - Lower CPU usage")
        else:
            print(f"❌ Failed to enable lightweight mode: {result}")
    
    if args.full_mode:
        print("\n🔬 Enabling full performance mode...")
        success, result = set_lightweight_mode(False)
        if success:
            print("✅ Full performance mode enabled")
            print("   - Advanced NLP processing enabled")
            print("   - More accurate sentiment analysis")
            print("   - Higher CPU usage")
        else:
            print(f"❌ Failed to enable full mode: {result}")
    
    if args.clear_cache:
        print("\n🗑️ Clearing sentiment analysis cache...")
        success, result = clear_cache()
        if success:
            print("✅ Cache cleared successfully")
            print(f"   Cache size: {result.get('cache_size', 0)} entries")
        else:
            print(f"❌ Failed to clear cache: {result}")

if __name__ == "__main__":
    main() 