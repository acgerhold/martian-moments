from airflow.decorators import task, dag
from airflow.operators import get_current_context
from datetime import datetime, timedelta
import logging
import sys
import os
import tempfile
import json

sys.path.append('/opt/airflow')
from src.assets.rover_assets import mars_photos_watcher
from src.utils.minio import get_minio_client, extract_json_as_jsonl_from_minio, get_recent_minio_files
from src.utils.snowflake import get_snowflake_connection, copy_photos_to_snowflake

@dag(
    dag_id="mars_photos_to_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule=[mars_photos_watcher],
    catchup=False,
    tags=["nasa", "photos", "snowflake", "bronze"],
)
def mars_photos_to_snowflake_dag():

    @task
    def get_files_from_kafka_events():
        
        context = get_current_context()
        logger = logging.getLogger(__name__)
        
        # Get the asset events (now from Kafka via AssetWatcher)
        triggering_asset_events = context.get('triggering_asset_events', [])
        logger.info(f"Processing triggered by {len(triggering_asset_events)} Kafka asset events")
        
        files_to_process = []
        
        for asset_event in triggering_asset_events:
            # Extract file path from Kafka event
            kafka_event = asset_event.get('extra', {})
            
            # MinIO Kafka events have this structure:
            # {"eventName": "s3:ObjectCreated:Put", "s3": {"object": {"key": "photos/..."}}}
            s3_info = kafka_event.get('s3', {})
            object_info = s3_info.get('object', {})
            file_path = object_info.get('key')
            
            if file_path and file_path.startswith('photos/') and file_path.endswith('.json'):
                files_to_process.append(file_path)
                logger.info(f"Found file to process from Kafka event: {file_path}")
        
        if not files_to_process:
            logger.warning("No valid files found in Kafka events, falling back to recent file check")
            # Fallback to recent files if Kafka events don't contain expected data
            minio_client = get_minio_client()
            files_to_process = get_recent_minio_files(minio_client, minutes_ago=10)
        
        logger.info(f"Total files to process: {len(files_to_process)}")
        return files_to_process
    
    @task
    def load_single_file_to_bronze(file_path: str):
        logger = logging.getLogger(__name__)
        logger.info(f"Processing file from Kafka event: {file_path}")
        
        try:
            minio_client = get_minio_client()
            snowflake_conn = get_snowflake_connection()
            
            # Extract JSON and convert to JSONL format
            jsonl_path = extract_json_as_jsonl_from_minio(minio_client, file_path)
            
            # Use cursor for Snowflake operations
            with snowflake_conn.cursor() as cursor:
                # Load data using COPY INTO
                copy_photos_to_snowflake(cursor, jsonl_path)
            
            # Extract metadata for return (read the original file for metadata)
            bucket = os.getenv('MINIO_BUCKET_NAME')
            tmp_dir = tempfile.gettempdir()
            tmp_filepath = os.path.join(tmp_dir, os.path.basename(file_path))
            
            try:
                minio_client.fget_object(bucket, file_path, tmp_filepath)
                with open(tmp_filepath, 'r') as f:
                    photo_data = json.load(f)
                
                result = {
                    "file_path": file_path,
                    "rover": photo_data.get("rover"),
                    "sol": photo_data.get("sol"),
                    "photo_count": photo_data.get("photo_count"),
                    "trigger_source": "kafka_event",
                    "status": "success"
                }
                
            finally:
                if os.path.exists(tmp_filepath):
                    os.remove(tmp_filepath)
            
            snowflake_conn.close()
            logger.info(f"Successfully loaded {file_path} to bronze table via Kafka trigger")
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return {
                "file_path": file_path,
                "trigger_source": "kafka_event",
                "status": "error",
                "error": str(e)
            }
    
    @task
    def send_processing_notification(load_results):
        logger = logging.getLogger(__name__)
        
        if hasattr(load_results, "resolve"):
            context = get_current_context()
            results = load_results.resolve(context)
        else:
            results = load_results
        
        successful = [r for r in results if r and r.get("status") == "success"]
        failed = [r for r in results if r and r.get("status") == "error"]
        
        summary = {
            "total_files": len(results),
            "successful": len(successful),
            "failed": len(failed),
            "trigger_source": "kafka_assetwatcher",
            "processed_timestamp": datetime.now().isoformat(),
            "successful_files": [r.get("file_path") for r in successful],
            "failed_files": [{"file": r.get("file_path"), "error": r.get("error")} for r in failed]
        }
        
        logger.info(f"Kafka-triggered bronze loading completed: {summary['successful']} successful, {summary['failed']} failed")
        
        if failed:
            logger.warning(f"Failed files: {[f['file'] for f in summary['failed_files']]}")
        
        # Optional: Send summary back to Kafka for monitoring
        # kafka_producer.send("mars-photos-processing-summary", summary)
        
        return summary
    
    files_from_kafka = get_files_from_kafka_events()    
    load_results = load_single_file_to_bronze.expand(file_path=files_from_kafka)    
    send_processing_notification(load_results)

dag = mars_photos_to_snowflake_dag()