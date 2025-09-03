from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from datetime import datetime, timezone
import json
import logging
import sys
from itertools import product

sys.path.append('/opt/airflow')
from src.ingestion import extract_photos_from_nasa
from src.utils.logger import setup_logger


ROVERS = ["Perseverance", "Curiosity"]
SOLS = [0, 1]

@dag(
    dag_id="mars_rover_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def mars_rover_ingestion_dag():
    @task
    def fetch_rover_photos(pair):
        rover = pair["rover"]
        sol = pair["sol"]
        logger = setup_logger("nasa_api.log", "ingestion")  
        logger.info(f"Running 'extract_photos_from_nasa' for rover: {rover} on sol: {sol}")
        print(f"Running 'extract_photos_from_nasa' for rover: {rover} on sol: {sol}")
        return extract_photos_from_nasa(rover, sol, logger)

    @task
    def consolidate_rover_photos_by_sol(results, sol):
        context = get_current_context()
        logger = setup_logger("nasa_api.log", "ingestion")
        logger.info(f"Running 'consolidate_rover_photos_by_sol' for sol: {sol}")
        print(f"Running 'consolidate_rover_photos_by_sol' for sol: {sol}")

        # Resolve all results
        if hasattr(results, "resolve"):
            all_photos = results.resolve(context)
        else:
            all_photos = results

        raw_photo_responses = []
        for r in all_photos:
            if r and r.get('sol') == sol:
                raw_photo_responses.append(r.get('photos', {}))

        ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        filename = f"raw_photo_response_sol_{sol}_{ingestion_timestamp}.json"

        enhanced_responses = {
            "photos": raw_photo_responses,
            "ingestion_date": ingestion_timestamp,
            "sol": sol,
            "filename": filename
        }

        return {filename: enhanced_responses}

    pairs = [{"rover": r, "sol": s} for r, s in product(ROVERS, SOLS)]
    fetch_results = fetch_rover_photos.expand(pair=pairs)

    consolidate_rover_photos_by_sol.expand(
        results=[fetch_results] * len(SOLS),
        sol=SOLS
    )

dag = mars_rover_ingestion_dag()

