from airflow.sdk import dag, task
from datetime import datetime, timezone
import sys

sys.path.append('/opt/airflow')
from src.ingestion import extract_photos_from_nasa, create_final_batch_json, generate_tasks_for_batch
from src.config import BATCH_1, BATCH_2, BATCH_3
from src.utils.minio import get_minio_client, upload_json_to_minio
from src.utils.logger import setup_logger

@dag(
    dag_id="mars_rover_photos_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Ingestion", "Photos", "MinIO", "Batched"]
)
def mars_rover_photos_ingestion_dag():
    
    @task
    def generate_tasks_for_batch_task():
        logger = setup_logger('get_ingestion_config_task', 'photo_ingestion_dag.log', 'ingestion')     
        tasks = generate_tasks_for_batch(BATCH_3, logger)  
        return tasks

    @task
    def fetch_and_collect_rover_photos_task(rover: str, sol: int):
        logger = setup_logger('fetch_and_collect_rover_photos_task', 'photo_ingestion_dag.log', 'ingestion')            
        photos_result = extract_photos_from_nasa(rover, sol, logger)
        return photos_result

    @task
    def create_combined_batch_file_task(all_rover_photos_results: list):
        logger = setup_logger('create_combined_batch_file_task', 'photo_ingestion_dag.log', 'ingestion')
        final_json = create_final_batch_json(BATCH_3, all_rover_photos_results, logger)
        minio_client = get_minio_client()
        upload_json_to_minio(minio_client, final_json, logger)

    config = generate_tasks_for_batch_task()
    all_rover_photos_results = fetch_and_collect_rover_photos_task.expand_kwargs(config)
    create_combined_batch_file_task(all_rover_photos_results)

dag = mars_rover_photos_ingestion_dag()