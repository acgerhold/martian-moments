import os
from io import BytesIO
from minio import Minio
import tempfile
import json

from src.config import MINIO_BUCKET

def get_minio_client():
    minio_client = Minio(
        os.getenv('MINIO_EXTERNAL_URL'),
        access_key = os.getenv('MINIO_ROOT_USER'),
        secret_key = os.getenv('MINIO_ROOT_PASSWORD'),
        secure = False
    )

    return minio_client

def upload_json_to_minio(minio_client, final_json, logger):

    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)

    filename = final_json['filename']
    minio_filepath = f"photos/{filename}"

    data_bytes = json.dumps(final_json).encode("utf-8")
    data_stream = BytesIO(data_bytes)

    logger.info(f"Uploading to MinIO - File: {filename}, Photos: {final_json['photo_count']}")
    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=minio_filepath,
        data=data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )    

def extract_json_as_jsonl_from_minio(minio_client, minio_filepath, logger):
    tmp_dir = tempfile.gettempdir()
    minio_filepath = minio_filepath.replace(f"{MINIO_BUCKET}/", "", 1)
    tmp_filepath = os.path.join(tmp_dir, os.path.basename(minio_filepath))

    minio_client.fget_object(MINIO_BUCKET, minio_filepath, tmp_filepath)
    logger.info(f"Extracted from MinIO - File: {minio_filepath}")
    
    with open(tmp_filepath, 'r') as f:
        data = json.load(f)
    
    jsonl_path = tmp_filepath.replace('.json', '.jsonl')
    with open(jsonl_path, 'w') as f:
        f.write(json.dumps(data) + '\n')
    
    os.remove(tmp_filepath)
    
    logger.info(f"Stored file - Path: {jsonl_path}")
    return jsonl_path