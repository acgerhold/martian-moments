from airflow.sdk import dag, task
from datetime import datetime, timezone
import sys

sys.path.append('/opt/airflow')
from src.ingestion.coordinates import extract_coordinates_from_nasa, create_final_coordinates_json, generate_tasks_for_coordinates_batch
from src.config import MARS_ROVERS_TRAVERSAL
from src.utils.minio import upload_json_to_minio
from src.utils.logger import setup_logger

@dag(
    dag_id="mars_rover_coordinates_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Ingestion", "Coordinates", "MinIO"]
)
def mars_rover_coordinates_ingestion_dag():

    @task
    def generate_tasks_for_batch_task():
        logger = setup_logger('get_ingestion_config_task', 'coordinate_ingestion_dag.log', 'ingestion')
        tasks = generate_tasks_for_coordinates_batch(MARS_ROVERS_TRAVERSAL, logger)
        return tasks
    
    @task
    def fetch_and_collect_rover_coordinates_task(rover: str):
        logger = setup_logger('fetch_and_collect_coordinates_task', 'coordinate_ingestion.log', 'ingestion')
        coordinates_result = extract_coordinates_from_nasa(rover, logger)
        return coordinates_result
    
    @task
    def create_combined_batch_file_task(all_rover_coordinate_results: list):
        logger = setup_logger('create_combined_batch_file_task', 'coordinate_ingestion_dag.log', 'ingestion')
        final_coordinates_json = create_final_coordinates_json(all_rover_coordinate_results, logger)
        upload_json_to_minio(final_coordinates_json, logger)

    
    config = generate_tasks_for_batch_task()
    all_rover_coordinate_results = fetch_and_collect_rover_coordinates_task.expand_kwargs(config)
    create_combined_batch_file_task(all_rover_coordinate_results)

dag = mars_rover_coordinates_ingestion_dag()