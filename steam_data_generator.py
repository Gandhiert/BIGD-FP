import pandas as pd
import json
import random
import numpy as np
from datetime import datetime, timedelta
import os
from faker import Faker
import xml.etree.ElementTree as ET
from xml.dom import minidom
import yaml
import re

class SteamDataGenerator:
    def __init__(self, csv_file_path):
        """
        Initialize the Steam Data Generator
        
        Args:
            csv_file_path (str): Path to the original Steam dataset CSV file
        """
        self.df = pd.read_csv(csv_file_path)
        self.fake = Faker()
        self.output_dir = "data_lake/raw"
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def generate_unstructured_text_reviews(self, num_reviews=10000):
        """
        Generate unstructured text data that simulates natural language reviews
        """
        review_templates = [
            "This game is absolutely {adjective}! I've been playing for {hours} hours and {opinion}. The {aspect} is {quality} and the {aspect2} really {verb}. {recommendation}",
            "After spending {hours} hours in this game, I can say that {opinion}. The {aspect} {verb} really well, but the {aspect2} could be better. {recommendation}",
            "{adjective} game overall. {opinion} The {aspect} is {quality} and I {verb} the {aspect2}. Would {recommendation_verb} to {audience}.",
            "I {verb} this game because {reason}. The {aspect} is {quality} and the {aspect2} {verb2}. {hours} hours played and {recommendation}.",
        ]
        
        adjectives = ["amazing", "terrible", "decent", "outstanding", "mediocre", "fantastic", "awful", "good", "bad", "incredible"]
        aspects = ["graphics", "gameplay", "story", "music", "controls", "mechanics", "design", "interface"]
        qualities = ["stunning", "poor", "average", "excellent", "disappointing", "solid", "weak", "impressive"]
        verbs = ["love", "hate", "enjoy", "dislike", "appreciate", "recommend", "avoid"]
        opinions = ["it's totally worth it", "I'm not impressed", "it exceeded my expectations", "it's okay for the price", "I regret buying it"]
        recommendations = ["Definitely recommend!", "Skip this one.", "Worth trying on sale.", "Buy it now!", "Wait for updates."]
        
        reviews = []
        
        for i in range(num_reviews):
            template = random.choice(review_templates)
            
            # Generate random values for template
            review_text = template.format(
                adjective=random.choice(adjectives),
                hours=random.randint(1, 500),
                opinion=random.choice(opinions),
                aspect=random.choice(aspects),
                aspect2=random.choice(aspects),
                quality=random.choice(qualities),
                verb=random.choice(verbs),
                verb2=random.choice(verbs),
                recommendation=random.choice(recommendations),
                recommendation_verb=random.choice(["recommend", "suggest"]),
                audience=random.choice(["everyone", "casual gamers", "hardcore players", "beginners"]),
                reason=random.choice(["of the amazing story", "it's so addictive", "the graphics are stunning", "it's really fun"])
            )
            
            # Add metadata
            review_data = {
                "review_id": f"rev_{i+1:06d}",
                "text": review_text,
                "timestamp": self.fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
                "helpful_votes": random.randint(0, 100),
                "total_votes": random.randint(0, 150),
                "playtime_hours": random.randint(1, 1000)
            }
            
            reviews.append(review_data)
        
        # Save as unstructured text file
        with open(f"{self.output_dir}/unstructured_reviews.txt", "w", encoding="utf-8") as f:
            for review in reviews:
                f.write(f"Review ID: {review['review_id']}\n")
                f.write(f"Date: {review['timestamp']}\n")
                f.write(f"Playtime: {review['playtime_hours']} hours\n")
                f.write(f"Helpful: {review['helpful_votes']}/{review['total_votes']}\n")
                f.write(f"Review: {review['text']}\n")
                f.write("-" * 80 + "\n\n")
        
        print(f"Generated {num_reviews} unstructured text reviews")
        return reviews
    
    def generate_semi_structured_json(self, sample_size=5000):
        """
        Generate semi-structured JSON data with nested objects and arrays
        """
        # Sample data from the original dataset
        sample_df = self.df.sample(n=min(sample_size, len(self.df)))
        
        json_data = []
        
        for idx, row in sample_df.iterrows():
            # Create a complex nested structure
            game_data = {
                "game_info": {
                    "basic": {
                        "name": str(row.iloc[0]) if not pd.isna(row.iloc[0]) else f"Game_{idx}",
                        "id": random.randint(100000, 999999),
                        "release_date": self.fake.date_between(start_date='-10y', end_date='today').isoformat()
                    },
                    "details": {
                        "genres": random.sample(["Action", "Adventure", "RPG", "Strategy", "Simulation", "Sports", "Racing", "Horror"], k=random.randint(1, 3)),
                        "tags": random.sample(["Indie", "Multiplayer", "Singleplayer", "Co-op", "Online", "Offline", "VR"], k=random.randint(2, 5)),
                        "platforms": random.sample(["Windows", "Mac", "Linux", "Steam Deck"], k=random.randint(1, 4))
                    },
                    "pricing": {
                        "base_price": round(random.uniform(5.99, 59.99), 2),
                        "current_price": round(random.uniform(2.99, 59.99), 2),
                        "discount_percentage": random.randint(0, 75),
                        "currency": "USD"
                    }
                },
                "user_metrics": {
                    "ratings": {
                        "overall_score": round(random.uniform(1.0, 5.0), 1),
                        "total_reviews": random.randint(50, 10000),
                        "positive_reviews": random.randint(30, 8000),
                        "review_distribution": {
                            "5_star": random.randint(0, 40),
                            "4_star": random.randint(0, 30),
                            "3_star": random.randint(0, 20),
                            "2_star": random.randint(0, 15),
                            "1_star": random.randint(0, 10)
                        }
                    },
                    "engagement": {
                        "avg_playtime_hours": round(random.uniform(1.0, 100.0), 1),
                        "peak_concurrent_players": random.randint(100, 50000),
                        "estimated_owners": f"{random.randint(1000, 1000000):,}",
                        "wishlist_count": random.randint(500, 100000)
                    }
                },
                "content_analysis": {
                    "sentiment_analysis": {
                        "positive_sentiment": round(random.uniform(0.3, 0.9), 3),
                        "negative_sentiment": round(random.uniform(0.1, 0.4), 3),
                        "neutral_sentiment": round(random.uniform(0.1, 0.3), 3)
                    },
                    "common_keywords": random.sample([
                        "fun", "addictive", "boring", "challenging", "easy", "difficult",
                        "graphics", "story", "gameplay", "music", "bugs", "glitches"
                    ], k=random.randint(3, 8)),
                    "recommendation_reasons": [
                        {"reason": "Great storyline", "frequency": random.randint(10, 200)},
                        {"reason": "Excellent graphics", "frequency": random.randint(5, 150)},
                        {"reason": "Fun gameplay", "frequency": random.randint(20, 300)}
                    ]
                },
                "metadata": {
                    "last_updated": datetime.now().isoformat(),
                    "data_source": "steam_api",
                    "confidence_score": round(random.uniform(0.7, 1.0), 3),
                    "flags": random.sample(["verified", "popular", "trending", "new_release"], k=random.randint(0, 2))
                }
            }
            
            json_data.append(game_data)
        
        # Save as JSON
        with open(f"{self.output_dir}/semi_structured_games.json", "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"Generated {len(json_data)} semi-structured JSON records")
        return json_data
    
    def generate_yaml_configuration_files(self, num_configs=200):
        """
        Generate YAML configuration files for different game setups
        """
        configs = []
        
        for i in range(num_configs):
            config = {
                'game_server': {
                    'name': f'steam_game_server_{i+1:03d}',
                    'host': self.fake.ipv4(),
                    'port': random.randint(7000, 9000),
                    'max_players': random.choice([16, 32, 64, 128]),
                    'region': random.choice(['us-east', 'us-west', 'eu-central', 'asia-pacific'])
                },
                'game_settings': {
                    'difficulty': random.choice(['easy', 'normal', 'hard', 'nightmare']),
                    'game_mode': random.choice(['pvp', 'pve', 'co-op', 'survival']),
                    'map_rotation': random.sample(['map1', 'map2', 'map3', 'map4', 'map5'], k=random.randint(2, 4)),
                    'round_time_minutes': random.randint(5, 30),
                    'friendly_fire': random.choice([True, False])
                },
                'anti_cheat': {
                    'enabled': True,
                    'provider': random.choice(['VAC', 'BattlEye', 'EasyAntiCheat']),
                    'logging_level': random.choice(['info', 'warning', 'debug']),
                    'auto_ban_threshold': random.randint(3, 10)
                },
                'performance': {
                    'tick_rate': random.choice([64, 128, 256]),
                    'bandwidth_limit_mbps': random.randint(10, 100),
                    'cpu_priority': random.choice(['normal', 'high', 'realtime']),
                    'memory_limit_gb': random.randint(2, 16)
                },
                'logging': {
                    'chat_logs': True,
                    'player_actions': True,
                    'system_events': True,
                    'rotation_days': random.randint(7, 30),
                    'compression': random.choice(['gzip', 'bzip2', 'none'])
                }
            }
            
            configs.append(config)
            
            # Save individual YAML files
            with open(f"{self.output_dir}/game_config_{i+1:03d}.yaml", "w") as f:
                yaml.dump(config, f, default_flow_style=False, indent=2)
        
        print(f"Generated {num_configs} YAML configuration files")
        return configs
    
    def generate_xml_structured_data(self, sample_size=2000):
        """
        Generate XML structured data for game catalog
        """
        root = ET.Element("game_catalog")
        root.set("version", "1.0")
        root.set("generated", datetime.now().isoformat())
        
        sample_df = self.df.sample(n=min(sample_size, len(self.df)))
        
        for idx, row in sample_df.iterrows():
            game = ET.SubElement(root, "game")
            game.set("id", str(random.randint(100000, 999999)))
            
            # Basic info
            basic_info = ET.SubElement(game, "basic_info")
            name = ET.SubElement(basic_info, "name")
            name.text = str(row.iloc[0]) if not pd.isna(row.iloc[0]) else f"Game_{idx}"
            
            developer = ET.SubElement(basic_info, "developer")
            developer.text = self.fake.company()
            
            release_date = ET.SubElement(basic_info, "release_date")
            release_date.text = self.fake.date_between(start_date='-10y', end_date='today').isoformat()
            
            # Categories
            categories = ET.SubElement(game, "categories")
            for genre in random.sample(["Action", "Adventure", "RPG", "Strategy", "Simulation"], k=random.randint(1, 3)):
                category = ET.SubElement(categories, "category")
                category.text = genre
            
            # Reviews summary
            reviews = ET.SubElement(game, "reviews_summary")
            total_reviews = ET.SubElement(reviews, "total_count")
            total_reviews.text = str(random.randint(50, 5000))
            
            avg_rating = ET.SubElement(reviews, "average_rating")
            avg_rating.text = str(round(random.uniform(1.0, 5.0), 1))
            
            recommendation_rate = ET.SubElement(reviews, "recommendation_rate")
            recommendation_rate.text = str(round(random.uniform(0.5, 0.95), 2))
            
            # Pricing
            pricing = ET.SubElement(game, "pricing")
            base_price = ET.SubElement(pricing, "base_price")
            base_price.set("currency", "USD")
            base_price.text = str(round(random.uniform(9.99, 59.99), 2))
            
            current_price = ET.SubElement(pricing, "current_price")
            current_price.set("currency", "USD")
            current_price.text = str(round(random.uniform(4.99, 59.99), 2))
        
        # Pretty print XML
        rough_string = ET.tostring(root, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent="  ")
        
        # Save XML file
        with open(f"{self.output_dir}/game_catalog.xml", "w", encoding="utf-8") as f:
            f.write(pretty_xml)
        
        print(f"Generated XML catalog with {sample_size} games")
        return pretty_xml
    
    def generate_log_files(self, num_days=365):
        """
        Generate unstructured log files simulating game server logs
        """
        log_levels = ["INFO", "WARNING", "ERROR", "DEBUG"]
        actions = [
            "Player joined server",
            "Player left server", 
            "Game started",
            "Game ended",
            "Player killed",
            "Player respawned",
            "Map changed",
            "Server restart",
            "Connection timeout",
            "Anti-cheat violation detected"
        ]
        
        for day in range(num_days):
            date = datetime.now() - timedelta(days=day)
            log_entries = []
            
            # Generate 100-500 log entries per day
            for _ in range(random.randint(100, 500)):
                timestamp = date + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                level = random.choice(log_levels)
                action = random.choice(actions)
                player_id = f"player_{random.randint(1000, 9999)}"
                server_id = f"srv_{random.randint(1, 10):02d}"
                
                # Create varied log formats
                if random.random() < 0.3:  # 30% chance of detailed log
                    log_entry = f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {level}: {action} - Player: {player_id}, Server: {server_id}, IP: {self.fake.ipv4()}, Ping: {random.randint(10, 200)}ms"
                else:
                    log_entry = f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {level}: {action} - {player_id}"
                
                log_entries.append((timestamp, log_entry))
            
            # Sort by timestamp
            log_entries.sort(key=lambda x: x[0])
            
            # Save log file
            filename = f"{self.output_dir}/game_server_{date.strftime('%Y%m%d')}.log"
            with open(filename, "w") as f:
                for _, entry in log_entries:
                    f.write(entry + "\n")
        
        print(f"Generated {num_days} days of log files")
    
    def generate_parquet_files(self):
        # games.parquet
        games = self.df.copy()
        games['app_id'] = range(1, len(games)+1)
        games.to_parquet("games.parquet")
        print("Generated games.parquet")

        # reviews.parquet (dummy)
        reviews = pd.DataFrame({
            "user_id": [f"user_{i}" for i in range(1, 11)],
            "app_id": [i for i in range(1, 11)],
            "helpful_votes": np.random.randint(0, 10, 10),
            "total_votes": np.random.randint(10, 20, 10),
            "playtime_hours": np.random.randint(1, 100, 10)
        })
        reviews.to_parquet("reviews.parquet")
        print("Generated reviews.parquet")

        # player_segments.parquet (dummy)
        segments = pd.DataFrame({
            "user_id": [f"user_{i}" for i in range(1, 11)],
            "cluster": np.random.randint(0, 3, 10)
        })
        import os
        os.makedirs("analytics", exist_ok=True)
        segments.to_parquet("analytics/player_segments.parquet")
        print("Generated analytics/player_segments.parquet")
    
    def generate_all_formats(self):
        """
        Generate all types of unstructured and semi-structured data
        """
        print("Starting comprehensive data generation...")
        print("=" * 50)
        
        # Generate unstructured data
        print("\n1. Generating unstructured text reviews...")
        self.generate_unstructured_text_reviews(10000)
        
        print("\n2. Generating unstructured log files...")
        self.generate_log_files(365)
        
        # Generate semi-structured data
        print("\n3. Generating semi-structured JSON data...")
        self.generate_semi_structured_json(5000)
        
        print("\n4. Generating YAML configuration files...")
        self.generate_yaml_configuration_files(200)
        
        print("\n5. Generating XML structured data...")
        self.generate_xml_structured_data(2000)
        
        print("\n6. Generating parquet files...")
        self.generate_parquet_files()
        
        print("\n" + "=" * 50)
        print("Data generation complete!")
        print(f"All files saved in: {self.output_dir}/")

# Example usage
if __name__ == "__main__":
    # Initialize the generator with your CSV file
    # Replace 'your_steam_dataset.csv' with the actual path to your downloaded CSV
    try:
        generator = SteamDataGenerator('dataset/games.csv')
        generator.generate_all_formats()
        generator.generate_parquet_files()
    except FileNotFoundError:
        print("Error: Please download the dataset from Kaggle and update the file path in the script.")
        print("The script expects a CSV file from the Steam game recommendations dataset.")
    except Exception as e:
        print(f"Error: {e}")
        print("Please ensure you have all required libraries installed:")
        print("pip install pandas numpy faker pyyaml")