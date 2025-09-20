import requests
from datetime import datetime, timezone

from src.config import PERSEVERANCE_TRAVERSAL_URL

def extract_coordinates_from_nasa(rover: str, logger):
    logger.info(f"Processing Coordinates Request - Rover: {rover}")
    match rover:
        case "Perseverance":
            coordinate_request = PERSEVERANCE_TRAVERSAL_URL

    try:
        response = requests.get(coordinate_request, timeout=30)
        response.raise_for_status()
        coordinate_response = response.json()

        enhanced_coordinate_response = {
            "rover": rover,
            "coordinate_response": coordinate_response
        }

        logger.info(f"Successful Coordinates Request - Rover: {rover}, Coordinate Count: {len(coordinate_response.get('features', []))}")
        return enhanced_coordinate_response
    except Exception as e:
        logger.error(f"Unsuccessful Coordinates Request - Rover: {rover}, Error: {e}")
        return {"features": []}
    
def create_final_coordinates_json(all_rover_coordinate_results, logger):
    logger.info("Creating Coordinates Batch File")
    all_rover_coordinate_results = list(all_rover_coordinate_results)
    all_coordinates = []
    for result in all_rover_coordinate_results:
        rover = result.get('rover')
        coordinate_response = result.get('coordinate_response', {})
        coordinates = coordinate_response.get('features', [])
        
        if coordinates:
            for coord in coordinates:
                coord['rover_name'] = rover
            all_coordinates.extend(coordinates)

    coordinate_count = len(all_coordinates)
    ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    filename = f"mars_rover_coordinates_{ingestion_timestamp}.json"

    final_coordinates_json = {
        "filename": filename,
        "coordinate_count": coordinate_count,
        "coordinates": all_coordinates,
        "ingestion_date": ingestion_timestamp
    }

    logger.info(f"Created Coordinates Batch - File: {filename}, Coordinate Count: {coordinate_count}")
    return final_coordinates_json

def generate_tasks_for_coordinates_batch(rovers, logger):
    logger.info("Generating Tasks for Coordinates DAG")
    tasks = []
    for rover in rovers:
        tasks.append({"rover": rover})

    logger.info(f"{len(tasks)} Tasks Generated")
    return tasks