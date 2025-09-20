import requests
from datetime import datetime, timezone

from src.config import NASA_KEY, PHOTOS_BASE_URL
    
def extract_photos_from_nasa(rover: str, sol: int, logger):
    logger.info(f"Processing Photos Request - Rover: {rover}, Sol: {sol}")
    photos_request = (
        f"{PHOTOS_BASE_URL}{rover}/photos"
        f"?sol={sol}&api_key={NASA_KEY}"
    )
    try:
        response = requests.get(photos_request, timeout=30)
        response.raise_for_status()
        photos_response = response.json()

        logger.info(f"Successful Photos Request - Rover: {rover}, Sol: {sol}, Photo Count: {len(photos_response.get('photos', []))}")
        return photos_response
    except Exception as e:
        logger.error(f"Unsuccessful Photos Request -  Rover: {rover}, Sol: {sol}, Error: {e}")
        return {"photos": []}
    
def create_final_photos_json(all_rover_photo_results, sol_range, logger):
        logger.info("Creating Photos Batch File")
        all_rover_photo_results = list(all_rover_photo_results)
        sol_start = min(sol_range)
        sol_end = max(sol_range)
        all_photos = []
        for result in all_rover_photo_results:
            photos = result.get('photos', [])
            if photos:
                all_photos.extend(photos)

        photo_count = len(all_photos)        
        ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        filename = f"mars_rover_photos_batch_sol_{sol_start}_to_{sol_end}_{ingestion_timestamp}.json"
                
        final_photos_json = {
            "filename": filename,
            "sol_start": sol_start,
            "sol_end": sol_end,
            "photo_count": photo_count,
            "photos": all_photos,
            "ingestion_date": ingestion_timestamp
        }

        logger.info(f"Created Photos Batch - File: {filename}, Photo Count: {photo_count}")
        return final_photos_json

def generate_tasks_for_photos_batch(ingestion_schedule, logger):
    logger.info("Generating Tasks for Photos DAG")
    tasks = []
    sol_start = 0
    sol_end = 0
    for rover_batch in ingestion_schedule:
        rover_name = rover_batch["rover_name"]

        if sol_start < rover_batch["sol_start"]:
            sol_start = rover_batch["sol_start"]

        if sol_end < rover_batch["sol_end"]:
            sol_end = rover_batch["sol_end"]

        sol_batch = list(range(rover_batch["sol_start"], rover_batch["sol_end"]))
        for sol in sol_batch:
            tasks.append({"rover": rover_name, "sol": sol})

    sol_range = list(range(sol_start, sol_end))

    logger.info(f"{len(tasks)} Tasks Generated")
    return {"tasks": tasks, "sol_range": sol_range}