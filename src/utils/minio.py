import os
from io import BytesIO
from minio import Minio
from dotenv import load_dotenv

def get_minio_client():
    minio_client = Minio(
        os.getenv('MINIO_EXTERNAL_URL'),
        access_key = os.getenv('MINIO_ACCESS_KEY'),
        secret_key = os.getenv('MINIO_SECRET_KEY'),
        secure = False
    )

    return minio_client

def upload_json_to_minio(minio_client, data):
    bucket = os.getenv('MINIO_BUCKET_NAME')

    file_names = []
    for file_name, json_data in data.items():
        data_bytes = json_data.encode("utf-8")
        data_stream = BytesIO(data_bytes)
        minio_client.put_object(
            bucket_name=bucket,
            object_name=file_name,
            data=data_stream,
            length=len(data_bytes),
            content_type="application/json"
        )
        file_names.append(file_name)
    
    return file_names