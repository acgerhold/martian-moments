from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import logging
import sys

sys.path.append('/opt/airflow')
from src.ingestion import extract_photos_from_nasa
from src.utils.logger import setup_logger

ROVERS = ["Perseverance"]
SOLS = [0, 1]

with DAG(
    dag_id="mars_rover_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def fetch_rover_photos(rover, sol):
        logger = setup_logger("nasa_api.log", "ingestion")  
        logger.info(f"Running 'extract_photos_from_nasa' for rover: {rover} on sol: {sol}")
        print(f"Running 'extract_photos_from_nasa' for rover: {rover} on sol: {sol}")
        return extract_photos_from_nasa(rover, sol, logger)

    @task
    def consolidate_rover_photos_by_sol(results, sol):
        logger = setup_logger("nasa_api.log", "ingestion")
        logger.info(f"Running 'consolidate_rover_photos_by_sol' for sol: {sol}")
        print(f"Running 'consolidate_rover_photos_by_sol' for sol: {sol}")

        combined_sol_results = {
            "sol": sol,
            "photos": results
        }
        return combined_sol_results

    for sol in SOLS:
        rover_results = fetch_rover_photos.expand(rover=ROVERS, sol=[sol]*len(ROVERS))
        consolidate_rover_photos_by_sol.override(task_id=f"consolidate_sol_{sol}")(rover_results, sol)

