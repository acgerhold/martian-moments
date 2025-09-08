from airflow.decorators import task, dag
from airflow.models import Variable
from datetime import datetime, timezone
import sys

sys.path.append('/opt/airflow')
from src.ingestion import extract_photos_from_nasa, create_final_json
from src.utils.minio import get_minio_client, upload_json_to_minio
from src.utils.logger import setup_logger

@dag(
    dag_id="mars_rover_photos_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nasa", "photos", "ingestion", "single"]
)
def mars_rover_photos_ingestion_dag():
    
    @task
    def get_ingestion_config():
        logger = setup_logger('get_ingestion_config_task', 'ingestion_dag.log', 'ingestion')

        logger.info("Retrieving ingestion config variables")
        rovers = Variable.get(
            "mars_rovers", 
            default_var=["Perseverance"], 
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

        logger.info(f"{len(tasks)} tasks scheduled for DAG run")   
        return tasks

    @task
    def fetch_and_store_rover_photos(rover: str, sol: int):
        logger = setup_logger('fetch_and_store_rover_photos_task', 'ingestion_dag.log', 'ingestion')

        logger.info(f"Fetching photos for {rover} on sol {sol}")   
        photos_result = extract_photos_from_nasa(rover, sol, logger)

        logger.info("Creating final .json and MinIO output path")
        final_json = create_final_json(rover, sol, photos_result)
        filepath = f"photos/{rover.lower()}/{final_json['filename']}"
            
        logger.info("Uploading to MinIO")        
        minio_client = get_minio_client()
        upload_json_to_minio(minio_client, filepath, final_json)

        logger.info(f"Stored {final_json['photo_count']} photos for {rover} on sol {sol}")

    config = get_ingestion_config()
    fetch_and_store_rover_photos.expand_kwargs(config)

dag = mars_rover_photos_ingestion_dag()