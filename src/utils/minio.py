import os
from io import BytesIO
from minio import Minio
import json
from dotenv import load_dotenv

def get_minio_client():
    load_dotenv()
    minio_client = Minio(
        os.getenv('MINIO_EXTERNAL_URL'),
        access_key = os.getenv('MINIO_ROOT_USER'),
        secret_key = os.getenv('MINIO_ROOT_PASSWORD'),
        secure = False
    )

    return minio_client

def upload_json_to_minio(minio_client, filepath, data):
    load_dotenv()
    bucket = os.getenv('MINIO_BUCKET')

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    data_bytes = json.dumps(data).encode("utf-8")
    data_stream = BytesIO(data_bytes)
    minio_client.put_object(
        bucket_name=bucket,
        object_name=filepath,
        data=data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )    

    return print("Potential Kafka Event")