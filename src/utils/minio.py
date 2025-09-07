import os
from io import BytesIO
from minio import Minio
import tempfile
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta

from src.utils.logger import setup_logger

load_dotenv()

def get_minio_client():
    minio_client = Minio(
        os.getenv('MINIO_EXTERNAL_URL'),
        access_key = os.getenv('MINIO_ROOT_USER'),
        secret_key = os.getenv('MINIO_ROOT_PASSWORD'),
        secure = False
    )

    return minio_client

def upload_json_to_minio(minio_client, minio_filepath, data):
    bucket = os.getenv('MINIO_BUCKET')

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    data_bytes = json.dumps(data).encode("utf-8")
    data_stream = BytesIO(data_bytes)
    minio_client.put_object(
        bucket_name=bucket,
        object_name=minio_filepath,
        data=data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )    

    return print("Potential Kafka Event")

def extract_json_as_jsonl_from_minio(minio_client, minio_filepath):
    bucket = os.getenv('MINIO_BUCKET_NAME')
    tmp_dir = tempfile.gettempdir()
    
    tmp_filepath = os.path.join(tmp_dir, os.path.basename(minio_filepath))
    minio_client.fget_object(bucket, minio_filepath, tmp_filepath)
    
    with open(tmp_filepath, 'r') as f:
        data = json.load(f)
    
    jsonl_path = tmp_filepath.replace('.json', '.jsonl')
    with open(jsonl_path, 'w') as f:
        f.write(json.dumps(data) + '\n')
    
    os.remove(tmp_filepath)
    
    return jsonl_path