from airflow import DAG
from airflow.decorators import task
from datetime import datetime

from src.ingestion import extract_manifest_from_nasa, extract_photos_from_nasa

ROVERS = ["curiosity", "opportunity", "spirit", "perseverance"]
SOLS = [0, 1, 2]

with DAG(
    dag_id="mars_rover_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    @task
    def fetch_rover_photos(rover, sol):
        return extract_photos_from_nasa(rover, sol)
    
    @task
    def consolidate_rover_photos_by_sol(results, sol):
        combined_sol_results = {
            "sol": sol,
            "photos": results
        }

        return combined_sol_results
    
    for sol in SOLS:
        rover_results = fetch_rover_photos.expand(rover=ROVERS, sol=[sol]*len(ROVERS))
        consolidate_rover_photos_by_sol.override(task_id=f"consolidate_sol_{sol}")(rover_results, sol)
