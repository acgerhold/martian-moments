from airflow.sdk import dag, task
from datetime import datetime
import sys

sys.path.append('/opt/airflow')
from src.ingestion.manifest import extract_manifests_from_nasa, create_final_manifest_json, generate_tasks_for_manifest_batch
from src.config import MARS_ROVERS
from src.utils.minio import upload_json_to_minio
from src.utils.logger import setup_logger

@dag(
    dag_id="mars_rover_manifest_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Ingestion", "Manifests", "MinIO"]
)
def mars_rover_photos_ingestion_dag():
    
    @task
    def generate_tasks_for_batch_task():
        logger = setup_logger('get_ingestion_config_task', 'manifest_ingestion_dag.log', 'ingestion')     
        tasks = generate_tasks_for_manifest_batch(MARS_ROVERS, logger)  
        return tasks

    @task
    def fetch_and_collect_rover_manifests_task(rover: str):
        logger = setup_logger('fetch_and_collect_rover_manifest_task', 'manifest_ingestion_dag.log', 'ingestion')            
        photos_result = extract_manifests_from_nasa(rover, logger)
        return photos_result

    @task
    def create_combined_batch_file_task(all_rover_manifest_results: list):
        logger = setup_logger('create_combined_batch_file_task', 'photo_ingestion_dag.log', 'ingestion')
        final_manifest_json = create_final_manifest_json(all_rover_manifest_results, logger)
        upload_json_to_minio(final_manifest_json, logger)


    config = generate_tasks_for_batch_task()
    all_rover_manifest_results = fetch_and_collect_rover_manifests_task.expand_kwargs(config)
    create_combined_batch_file_task(all_rover_manifest_results)

dag = mars_rover_photos_ingestion_dag()