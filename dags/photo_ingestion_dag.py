from airflow.sdk import dag, task
from datetime import datetime
import sys

sys.path.append('/opt/airflow')
from src.ingestion.photos import extract_photos_from_nasa, create_final_photos_json, generate_tasks_for_photos_batch
from src.config import MARS_ROVERS, SOL_BATCH
from src.utils.minio import upload_json_to_minio
from src.utils.logger import setup_logger

@dag(
    dag_id="mars_rover_photos_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Ingestion", "Photos", "MinIO"]
)
def mars_rover_photos_ingestion_dag():
    
    @task
    def generate_tasks_for_batch_task():
        logger = setup_logger('get_ingestion_config_task', 'photo_ingestion_dag.log', 'ingestion')     
        tasks = generate_tasks_for_photos_batch(MARS_ROVERS, SOL_BATCH, logger)  
        return tasks

    @task
    def fetch_and_collect_rover_photos_task(rover: str, sol: int):
        logger = setup_logger('fetch_and_collect_rover_photos_task', 'photo_ingestion_dag.log', 'ingestion')            
        photos_result = extract_photos_from_nasa(rover, sol, logger)
        return photos_result

    @task
    def create_combined_batch_file_task(all_rover_photo_results: list):
        logger = setup_logger('create_combined_batch_file_task', 'photo_ingestion_dag.log', 'ingestion')
        final_photos_json = create_final_photos_json(SOL_BATCH, all_rover_photo_results, logger)
        upload_json_to_minio(final_photos_json, logger)


    config = generate_tasks_for_batch_task()
    all_rover_photo_results = fetch_and_collect_rover_photos_task.expand_kwargs(config)
    create_combined_batch_file_task(all_rover_photo_results)

dag = mars_rover_photos_ingestion_dag()