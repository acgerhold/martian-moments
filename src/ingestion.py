import requests
import os
from dotenv import load_dotenv

def extract_photos_from_nasa(rover: str, sol: int):
    load_dotenv()
    photos_request = (
        f"https://api.nasa.gov/mars-photos/api/v1/rovers/{rover}/photos"
        f"?sol={sol}&api_key={os.getenv('NASA_KEY')}"
    )
    try:
        response = requests.get(photos_request, timeout=30)
        response.raise_for_status()
        photos_response = response.json()
    except requests.RequestException as e:
        print(f"Error fetching photos for rover {rover} on sol {sol}: {e}")
        # photos_response = {"error": str(e), "rover": rover, "sol": sol}

    return photos_response

def extract_manifest_from_nasa(rover: str):
    load_dotenv()
    manifest_request = (
        f"https://api.nasa.gov/mars-photos/api/v1/manifests/{rover}"
        f"?api_key={os.getenv('NASA_KEY')}"
    )
    try:
        response = requests.get(manifest_request, timeout=30)
        response.raise_for_status()
        manifest_response = response.json()
    except requests.RequestException as e:
        print(f"Error fetching manifest for rover {rover}: {e}")
        # manifest_response = {"error": str(e), "rover": rover}

    return manifest_response