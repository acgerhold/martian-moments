import requests
from datetime import datetime, timezone

from src.config import NASA_KEY, PHOTOS_BASE_URL
    
def extract_photos_from_nasa(rover: str, sol: int, logger):
    logger.info(f"Processing photos request for rover: {rover} on sol: {sol}")
    photos_request = (
        f"{PHOTOS_BASE_URL}{rover}/photos"
        f"?sol={sol}&api_key={NASA_KEY}"
    )
    try:
        response = requests.get(photos_request, timeout=30)
        response.raise_for_status()
        photos_response = response.json()

        logger.info(f"Fetched {len(photos_response.get('photos', []))} photos for {rover} on sol {sol}")
        return photos_response
    except Exception as e:
        logger.error(f"Error processing photos request for rover: {rover} on sol: {sol}: {e}")
        return {"photos": []}
    
def create_final_photos_json(batch, all_rover_photo_results, logger):
        logger.info("Creating photos final .json")
        all_rover_photo_results = list(all_rover_photo_results)
        sol_start = min(batch)
        sol_end = max(batch)
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

        logger.info(f"Created file - Name: {filename}, Date: {ingestion_timestamp}, Photo Count: {photo_count}")
        return final_photos_json

def generate_tasks_for_photos_batch(rovers, sol_batch, logger):
    logger.info("Generating tasks for photos DAG run")
    tasks = []
    for rover in rovers:
        for sol in sol_batch:
            tasks.append({"rover": rover, "sol": sol})

    logger.info(f"{len(tasks)} tasks scheduled for this DAG run")
    return tasks