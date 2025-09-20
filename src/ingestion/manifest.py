import requests
from datetime import datetime, timezone

from src.config import NASA_KEY, MANIFEST_BASE_URL

def extract_manifests_from_nasa(rover: str, logger):
    logger.info(f"Processing Manifest Request - Rover: {rover}")
    manifest_request = (
        f"{MANIFEST_BASE_URL}{rover}"
        f"?api_key={NASA_KEY}"
    )
    try:
        response = requests.get(manifest_request, timeout=30)
        response.raise_for_status()
        manifest_response = response.json()

        logger.info(f"Successful Manifest Request - Rover: {rover}")
        return manifest_response
    except Exception as e:
        logger.error(f"Unsuccessful Manifest Request - Rover: {rover}, Error: {e}")
        return {"photo_manifest": []}
    
def create_final_manifest_json(all_rover_manifest_results, logger):
        logger.info("Creating Manifest Batch File")
        all_rover_manifest_results = list(all_rover_manifest_results)
        all_manifests = []
        for result in all_rover_manifest_results:
            manifest = result.get('photo_manifest', {})
            if manifest:
                all_manifests.append(manifest)

        ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        filename = f"mars_rover_manifests_{ingestion_timestamp}.json"
                
        final_manifest_json = {
            "filename": filename,
            "manifests": all_manifests,
            "ingestion_date": ingestion_timestamp
        }

        logger.info(f"Created Manifest Batch - File: {filename}")
        return final_manifest_json

def generate_tasks_for_manifest_batch(rovers, logger):
    logger.info("Generating Tasks for Manifest DAG")
    tasks = []
    for rover in rovers:
        tasks.append({"rover": rover})

    logger.info(f"{len(tasks)} Tasks Generated")
    return tasks