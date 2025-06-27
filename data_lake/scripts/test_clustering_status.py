#!/usr/bin/env python3
"""
Script untuk testing clustering status dan diagnosis masalah verification
"""

import requests
import json
from minio import Minio
from datetime import datetime, timedelta
import sys

def test_api_status():
    """Test API clustering status endpoint"""
    print("🔍 Testing API clustering status...")
    
    try:
        response = requests.get("http://localhost:5000/api/clustering/status", timeout=30)
        if response.status_code == 200:
            status_data = response.json()
            print("✅ API responded successfully")
            print("📊 Status data:")
            print(json.dumps(status_data, indent=2))
            return status_data
        else:
            print(f"❌ API error: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ API request failed: {e}")
        return None

def test_minio_files():
    """Test MinIO cluster files directly"""
    print("\n🔍 Testing MinIO cluster files directly...")
    
    try:
        client = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
        
        # Check if bucket exists
        if not client.bucket_exists('clusters-zone'):
            print("❌ clusters-zone bucket does not exist")
            return
        
        print("✅ clusters-zone bucket exists")
        
        # List latest files
        objects = list(client.list_objects('clusters-zone', prefix='latest/', recursive=True))
        
        if not objects:
            print("❌ No files found in latest/ folder")
            return
        
        print(f"📁 Found {len(objects)} files in latest/ folder:")
        
        for obj in objects:
            print(f"   - {obj.object_name}")
            print(f"     Size: {obj.size} bytes")
            print(f"     Modified: {obj.last_modified}")
            
            # Check freshness
            if obj.last_modified:
                time_diff = datetime.now() - obj.last_modified.replace(tzinfo=None)
                hours_old = time_diff.total_seconds() / 3600
                minutes_old = time_diff.total_seconds() / 60
                
                print(f"     Age: {hours_old:.2f} hours ({minutes_old:.1f} minutes)")
                
                if time_diff < timedelta(minutes=10):
                    print("     🟢 Very fresh (< 10 minutes)")
                elif time_diff < timedelta(hours=1):
                    print("     🟡 Fresh (< 1 hour)")
                elif time_diff < timedelta(hours=12):
                    print("     🟠 Acceptable (< 12 hours)")
                else:
                    print("     🔴 Stale (> 12 hours)")
            print()
        
    except Exception as e:
        print(f"❌ MinIO test failed: {e}")

def test_clustering_api():
    """Test clustering games API"""
    print("🔍 Testing clustering games API...")
    
    try:
        response = requests.get("http://localhost:5000/api/clustering/games?limit=5", timeout=30)
        if response.status_code == 200:
            games_data = response.json()
            print("✅ Clustering games API responded successfully")
            
            total_games = games_data.get('pagination', {}).get('total', 0)
            returned_games = len(games_data.get('games', []))
            
            print(f"📊 Total games: {total_games}")
            print(f"📊 Returned games: {returned_games}")
            
            if returned_games > 0:
                print("📋 Sample games:")
                for i, game in enumerate(games_data['games'][:3]):
                    print(f"   {i+1}. {game.get('title', 'N/A')} (Cluster: {game.get('cluster', 'N/A')})")
            
            return True
        else:
            print(f"❌ Games API error: {response.status_code}")
            if response.text:
                print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Games API test failed: {e}")
        return False

def main():
    """Main diagnostic function"""
    print("🎮 CLUSTERING STATUS DIAGNOSTIC")
    print("=" * 50)
    
    # Test API status
    api_status = test_api_status()
    
    # Test MinIO files directly
    test_minio_files()
    
    # Test clustering games API
    api_working = test_clustering_api()
    
    print("\n📋 DIAGNOSIS SUMMARY")
    print("=" * 30)
    
    if api_status:
        clustering_status = api_status.get('clustering_status', 'unknown')
        print(f"API Status: {clustering_status}")
        
        if clustering_status == 'stale':
            freshness = api_status.get('data_freshness', {}).get('clusters', {})
            hours_old = freshness.get('hours_old', 0)
            print(f"⚠️ Data is {hours_old:.2f} hours old")
            print("💡 Possible solutions:")
            print("   1. Files might be older than 12 hour threshold")
            print("   2. Timezone mismatch between MinIO and API")
            print("   3. Clustering job might not have saved files properly")
        elif clustering_status == 'healthy':
            print("✅ Clustering system is healthy")
        elif clustering_status == 'no_data':
            print("❌ No clustering data found")
        else:
            print(f"❓ Unknown status: {clustering_status}")
    else:
        print("❌ API not responding")
    
    if api_working:
        print("✅ Clustering data is accessible via API")
    else:
        print("❌ Clustering data not accessible via API")

if __name__ == "__main__":
    main() 