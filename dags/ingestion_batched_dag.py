from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime, timezone
import sys

sys.path.append('/opt/airflow')
from src.ingestion import extract_photos_from_nasa, create_final_batch_json
from src.utils.minio import get_minio_client, upload_json_to_minio
from src.utils.logger import setup_logger

@dag(
    dag_id="mars_rover_photos_ingestion_batched",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nasa", "photos", "ingestion", "batched"]
)
def mars_rover_photos_ingestion_batched_dag():
    
    @task
    def get_ingestion_config():
        logger = setup_logger('get_ingestion_config_task', 'ingestion_batched_dag.log', 'ingestion')

        logger.info("Retrieving ingestion config variables")
        rovers = Variable.get(
            "mars_rovers", 
            default_var=["Perseverance", "Curiosity", "Opportunity", "Spirit"], 
            deserialize_json=True
        )
        sols = Variable.get(
            "mars_sols", 
            default_var=list(range(0, 1)), 
            deserialize_json=True
        )
        logger.info(f"'mars_rovers': {rovers} - 'mars_sols': {sols}")
        
        logger.info("Creating tasks for DAG run")
        tasks = []
        for rover in rovers:
            for sol in sols:
                tasks.append({"rover": rover, "sol": sol})

        logger.info(f"{len(tasks)} tasks scheduled for this DAG run")  
        return tasks

    @task
    def fetch_and_collect_rover_photos(rover: str, sol: int):
        logger = setup_logger('fetch_and_collect_rover_photos_task', 'ingestion_batched_dag.log', 'ingestion')
            
        logger.info(f"Fetching photos for {rover} on sol {sol}")    
        photos_result = extract_photos_from_nasa(rover, sol, logger)

        logger.info(f"Fetched {photos_result['photo_count']} photos for {rover} on sol {sol}")        
        return photos_result

    @task
    def create_combined_batch_file(all_rover_photos_results: list):
        logger = setup_logger('create_combined_batch_file_task', 'ingestion_batched_dag.log', 'ingestion')

        logger.info("Retrieving variables from Airflow")
        sols = Variable.get(
            "mars_sols", 
            default_var=list(range(0, 1)), 
            deserialize_json=True
        )

        logger.info("Creating final batch .json and MinIO output path")
        all_rover_photos_results = list(all_rover_photos_results)
        final_json = create_final_batch_json(sols, all_rover_photos_results)
        filepath = f"photos/batched/{final_json['filename']}"
        
        logger.info("Uploading to MinIO")        
        minio_client = get_minio_client()
        upload_json_to_minio(minio_client, filepath, final_json)

        logger.info(f"Stored {final_json['photo_count']} total photos across {len(all_rover_photos_results)} rover/sol combinations")

    config = get_ingestion_config()
    all_results = fetch_and_collect_rover_photos.expand_kwargs(config)
    create_combined_batch_file(all_results)

dag = mars_rover_photos_ingestion_batched_dag()