from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime, timezone
import sys

sys.path.append('/opt/airflow')
from src.ingestion import extract_photos_from_nasa
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

        logger.info("Retrieving variables from Airflow")
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
        logger.info(f"Received ingestion configuration - 'mars_rovers': {rovers} - 'mars_sols': {sols}")
        
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
        photos_data = extract_photos_from_nasa(rover, sol, logger)
                
        if photos_data:
            ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            filename = f"{rover.lower()}_photos_sol_{sol}_{ingestion_timestamp}.json"
            
            enhanced_data = {
                "filename": filename,
                "sol_start": sol,
                "sol_end": sol,
                "photo_count": len(photos_data.get('photos', [])),
                "photos": photos_data.get('photos', []),
                "ingestion_date": ingestion_timestamp
            }
            logger.info(f"Fetched {enhanced_data['photo_count']} photos for {rover} on sol {sol}")

            return enhanced_data
        else:
            logger.warning(f"No photos found for rover: {rover} on sol: {sol}")
            return {
                "filename": filename, 
                "sol_start": sol,
                "sol_end": sol,
                "photo_count": 0, 
                "photos": [],
                "ingestion_date": ingestion_timestamp
            }

    @task
    def create_combined_batch_file(all_rover_results: list):
        logger = setup_logger('create_combined_batch_file_task', 'ingestion_batched_dag.log', 'ingestion')
    
        logger.info("Retrieving variables from Airflow")
        sols = Variable.get(
            "mars_sols", 
            default_var=list(range(0, 1)), 
            deserialize_json=True
        )
        sol_start = min(sols)
        sol_end = max(sols)
        logger.info(f"Creating batch file for sols {sol_start} to {sol_end}")
        
        all_rover_results = list(all_rover_results)
        
        ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        total_photos = sum(result["photo_count"] for result in all_rover_results)
        if sol_start == sol_end:
            filename = f"mars_rover_photos_batch_sol_{sol_start}_{ingestion_timestamp}.json"
        else:
            filename = f"mars_rover_photos_batch_sol_{sol_start}_to_{sol_end}_{ingestion_timestamp}.json"
                
        final_json = {
            "filename": filename,
            "sol_start": sol_start,
            "sol_end": sol_end,
            "photo_count": sum(result["photo_count"] for result in all_rover_results),
            "photos": all_rover_results,
            "ingestion_date": ingestion_timestamp
        }
        
        logger.info("Uploading to MinIO")
        filepath = f"photos/batched/{filename}"        
        minio_client = get_minio_client()
        upload_json_to_minio(minio_client, filepath, final_json)
        logger.info(f"Stored {total_photos} total photos across {len(all_rover_results)} rover/sol combinations")

        return {
            "filename": filename,
            "filepath": filepath,
            "total_records": len(all_rover_results),
            "total_photos": total_photos,
            "status": "success"
        }

    config = get_ingestion_config()
    all_results = fetch_and_collect_rover_photos.expand_kwargs(config)
    
    batch_result = create_combined_batch_file(all_results)

dag = mars_rover_photos_ingestion_batched_dag()