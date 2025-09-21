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

def upload_json_to_minio(final_json, logger):
    logger.info(f"Attempting upload to MinIO - {final_json['filename']}")
    minio_client = get_minio_client()
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)

    filename = final_json['filename']
    match filename:
        case name if name.startswith("mars_rover_photos"):
            minio_filepath = f"photos/{filename}"
        case name if name.startswith("mars_rover_coordinates"):
            minio_filepath = f"coordinates/{filename}"
        case name if name.startswith("mars_rover_manifests"):
            minio_filepath = f"manifests/{filename}"
        case _:
            minio_filepath = f"{filename}"

    data_bytes = json.dumps(final_json).encode("utf-8")
    data_stream = BytesIO(data_bytes)

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=minio_filepath,
        data=data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    logger.info(f"Uploaded to MinIO - File: {filename}")    

def extract_json_as_jsonl_from_minio(minio_upload_path, logger):
    minio_client = get_minio_client()
    logger.info(f"Attempting extract from MinIO - Path: {minio_upload_path}")
    tmp_dir = tempfile.gettempdir()
    minio_file = minio_upload_path.replace(f"{MINIO_BUCKET}/", "", 1)
    tmp_staging_path = os.path.join(tmp_dir, os.path.basename(minio_file))

    minio_client.fget_object(MINIO_BUCKET, minio_file, tmp_staging_path)
    logger.info(f"Extracted from MinIO - File: {minio_upload_path}")
    
    with open(tmp_staging_path, 'r') as f:
        data = json.load(f)
    
    tmp_jsonl_staging_path = tmp_staging_path.replace('.json', '.jsonl')
    with open(tmp_jsonl_staging_path, 'w') as f:
        f.write(json.dumps(data) + '\n')
    
    os.remove(tmp_staging_path)
    
    return tmp_jsonl_staging_path