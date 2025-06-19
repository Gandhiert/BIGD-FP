import os
from minio import Minio
import pandas as pd
import yaml
import xml.etree.ElementTree as ET
import re
from io import BytesIO

RAW_DIR = '../raw'
WAREHOUSE_DIR = '../warehouse'
BUCKET = 'raw-zone'

client = Minio(
    'localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

def download_all_raw():
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR)
    objects = client.list_objects(BUCKET, recursive=True)
    for obj in objects:
        dest_path = os.path.join(RAW_DIR, obj.object_name)
        print(f'Downloading {obj.object_name} ...')
        client.fget_object(BUCKET, obj.object_name, dest_path)
    print('Semua file raw berhasil didownload dari MinIO!')

def etl_reviews():
    src = os.path.join(RAW_DIR, 'unstructured_reviews.txt')
    dst = os.path.join(WAREHOUSE_DIR, 'reviews.parquet')
    reviews = []
    with open(src, encoding='utf-8') as f:
        review = {}
        for line in f:
            line = line.strip()
            if line.startswith('Review ID:'):
                review['review_id'] = line.split(':',1)[1].strip()
            elif line.startswith('Date:'):
                review['timestamp'] = line.split(':',1)[1].strip()
            elif line.startswith('Playtime:'):
                match = re.search(r'(\d+)', line)
                review['playtime_hours'] = int(match.group(1)) if match else None
            elif line.startswith('Helpful:'):
                nums = re.findall(r'(\d+)', line)
                if len(nums) == 2:
                    review['helpful_votes'] = int(nums[0])
                    review['total_votes'] = int(nums[1])
                else:
                    review['helpful_votes'] = None
                    review['total_votes'] = None
            elif line.startswith('Review:'):
                review['text'] = line.split(':',1)[1].strip()
            elif line.startswith('--------------------------------------------------------------------------------'):
                if review:
                    reviews.append(review)
                    review = {}
    df = pd.DataFrame(reviews)
    df.to_parquet(dst)
    print('reviews.parquet berhasil dibuat')

def etl_games():
    src = os.path.join(RAW_DIR, 'semi_structured_games.json')
    dst = os.path.join(WAREHOUSE_DIR, 'games.parquet')
    df = pd.read_json(src)
    df.to_parquet(dst)
    print('games.parquet berhasil dibuat')

def etl_configs():
    dst = os.path.join(WAREHOUSE_DIR, 'configs.parquet')
    yaml_files = [f for f in os.listdir(RAW_DIR) if f.startswith('game_config_') and f.endswith('.yaml')]
    configs = []
    for file in yaml_files:
        with open(os.path.join(RAW_DIR, file)) as f:
            data = yaml.safe_load(f)
            flat = {}
            for k, v in data.items():
                if isinstance(v, dict):
                    for kk, vv in v.items():
                        flat[f'{k}_{kk}'] = vv
                else:
                    flat[k] = v
            configs.append(flat)
    df = pd.DataFrame(configs)
    df.to_parquet(dst)
    print('configs.parquet berhasil dibuat')

def etl_catalog():
    src = os.path.join(RAW_DIR, 'game_catalog.xml')
    dst = os.path.join(WAREHOUSE_DIR, 'catalog.parquet')
    tree = ET.parse(src)
    root = tree.getroot()
    games = []
    for game in root.findall('game'):
        d = {}
        d['id'] = game.get('id')
        basic = game.find('basic_info')
        d['name'] = basic.find('name').text if basic is not None else None
        d['developer'] = basic.find('developer').text if basic is not None else None
        d['release_date'] = basic.find('release_date').text if basic is not None else None
        reviews = game.find('reviews_summary')
        if reviews is not None:
            d['total_reviews'] = reviews.find('total_count').text if reviews.find('total_count') is not None else None
            d['avg_rating'] = reviews.find('average_rating').text if reviews.find('average_rating') is not None else None
            d['recommendation_rate'] = reviews.find('recommendation_rate').text if reviews.find('recommendation_rate') is not None else None
        pricing = game.find('pricing')
        if pricing is not None:
            d['base_price'] = pricing.find('base_price').text if pricing.find('base_price') is not None else None
            d['current_price'] = pricing.find('current_price').text if pricing.find('current_price') is not None else None
        games.append(d)
    df = pd.DataFrame(games)
    df.to_parquet(dst)
    print('catalog.parquet berhasil dibuat')

def etl_logs():
    dst = os.path.join(WAREHOUSE_DIR, 'logs.parquet')
    log_files = [f for f in os.listdir(RAW_DIR) if f.startswith('game_server_') and f.endswith('.log')]
    log_entries = []
    for file in log_files:
        with open(os.path.join(RAW_DIR, file)) as f:
            for line in f:
                m = re.match(r'\[(.*?)\] (\w+): (.*?) - (.*)', line)
                if m:
                    timestamp, level, action, rest = m.groups()
                    entry = {'timestamp': timestamp, 'level': level, 'action': action}
                    player_id = re.search(r'player_\d+', rest)
                    if player_id:
                        entry['player_id'] = player_id.group(0)
                    else:
                        entry['player_id'] = None
                    log_entries.append(entry)
    df = pd.DataFrame(log_entries)
    df.to_parquet(dst)
    print('logs.parquet berhasil dibuat')

if __name__ == '__main__':
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)
    download_all_raw()
    etl_reviews()
    etl_games()
    etl_configs()
    etl_catalog()
    etl_logs()
    print('ETL dari MinIO ke Parquet selesai!') 