import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.config import NASA_KEY, MARS_ROVERS, PHOTOS_BASE_URL, MANIFEST_BASE_URL

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
        return photos_response
    except Exception as e:
        error_msg = f"Error processing photos request for rover: {rover} on sol: {sol}: {e}"
        logger.error(error_msg)
        return {"photos": []}

def extract_manifest_from_nasa(rover: str, logger):
    logger.info(f"Processing manifest request for rover: {rover}")

    load_dotenv()
    manifest_request = (
        f"{MANIFEST_BASE_URL}{rover}"
        f"?api_key={NASA_KEY}"
    )
    try:
        response = requests.get(manifest_request, timeout=30)
        response.raise_for_status()
        manifest_response = response.json()
        return manifest_response
    except Exception as e:
        error_msg = f"Error processing manifest request for rover {rover}: {e}"
        logger.error(error_msg)
        return {"manifest": []}
    
def create_final_json(rover, sol, photos_result):
    if photos_result:
        photo_count = len(photos_result.get('photos', []))
        photos = photos_result.get('photos', [])
        ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        filename = f"{rover.lower()}_photos_sol_{sol}_{ingestion_timestamp}.json"
        
        final_json = {
            "filename": filename,
            "sol_start": sol,
            "sol_end": sol,
            "photo_count": photo_count,
            "photos": photos,
            "ingestion_date": ingestion_timestamp,
        }

        return final_json
    
def create_final_batch_json(batch, all_rover_photos_results):
        sol_start = min(batch)
        sol_end = max(batch)

        all_photos = []
        for result in all_rover_photos_results:
            photos = result.get('photos', [])
            if photos:
                all_photos.extend(photos)

        photo_count = len(all_photos)        
        ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        filename = f"mars_rover_photos_batch_sol_{sol_start}_to_{sol_end}_{ingestion_timestamp}.json"
                
        final_json = {
            "filename": filename,
            "sol_start": sol_start,
            "sol_end": sol_end,
            "photo_count": photo_count,
            "photos": all_photos,
            "ingestion_date": ingestion_timestamp
        }

        return final_json

def generate_tasks_for_batch(batch):
    tasks = []
    for rover in MARS_ROVERS:
        for sol in batch:
            tasks.append({"rover": rover, "sol": sol})

    return tasks