import os
from minio import Minio

WAREHOUSE_DIR = '../warehouse'
BUCKET = 'warehouse-zone'

client = Minio(
    'localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

def upload_all_warehouse():
    for fname in os.listdir(WAREHOUSE_DIR):
        fpath = os.path.join(WAREHOUSE_DIR, fname)
        if os.path.isfile(fpath):
            print(f'Uploading {fname} ...')
            client.fput_object(BUCKET, fname, fpath)
    print('Semua file warehouse berhasil diupload ke MinIO!')

if __name__ == '__main__':
    # Pastikan bucket sudah ada
    found = client.bucket_exists(BUCKET)
    if not found:
        client.make_bucket(BUCKET)
    upload_all_warehouse() 