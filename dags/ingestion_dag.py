from airflow.models import Variable
from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timezone
import logging
import sys

sys.path.append('/opt/airflow')
from src.ingestion import extract_photos_from_nasa
from src.utils.minio import get_minio_client, upload_json_to_minio
from src.utils.logger import setup_logger

@dag(
    dag_id="mars_rover_photos_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nasa", "photos", "ingestion"]
)
def mars_rover_photos_ingestion_dag():
    
    @task
    def get_ingestion_config():
        rovers = Variable.get(
            "mars_rovers", 
            default_var=["Perseverance"], 
            deserialize_json=True
        )
        sols = Variable.get(
            "mars_sols", 
            default_var=list(range(0, 2)), 
            deserialize_json=True
        )
        
        logging.info(f"Recieved ingestion configuration - 'mars_rovers': {rovers} - 'mars_sols': {sols}")
        
        tasks = []
        for rover in rovers:
            for sol in sols:
                tasks.append({"rover": rover, "sol": sol})
        
        logging.info(f"{len(tasks)} tasks scheduled for this DAG run")
        
        return tasks

    @task
    def fetch_and_store_rover_photos(rover: str, sol: int):
        logger = setup_logger("ingestion_dag.log", "ingestion")
        logger.info(f"Fetching photos for rover: {rover} on sol: {sol}")
        
        photos_data = extract_photos_from_nasa(rover, sol, logger)
        
        if photos_data:
            ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            filename = f"{rover.lower()}_photos_sol_{sol}_{ingestion_timestamp}.json"
            
            enhanced_data = {
                "photos": photos_data.get('photos', []),
                "rover": rover,
                "sol": sol,
                "ingestion_date": ingestion_timestamp,
                "photo_count": len(photos_data.get('photos', []))
            }
            
            filepath = f"{rover.lower()}/{filename}"
            minio_client = get_minio_client()
            upload_json_to_minio(minio_client, filepath, enhanced_data)

            logger.info(f"Successfully stored {enhanced_data['photo_count']} photos for rover: {rover} on sol: {sol}")
            
            return {
                "filename": filename, 
                "rover": rover, 
                "sol": sol,
                "photo_count": enhanced_data['photo_count'],
                "status": "success"
            }
        else:
            logger.warning(f"No photos found for rover: {rover} on sol: {sol}")
            return {
                "rover": rover, 
                "sol": sol, 
                "status": "no_data"
            }

    config = get_ingestion_config()
    fetch_and_store_rover_photos.expand_kwargs(config)

dag = mars_rover_photos_ingestion_dag()