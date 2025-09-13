from dotenv import load_dotenv
import os

load_dotenv()

NASA_KEY = os.getenv('NASA_KEY')

MARS_ROVERS = ["Perseverance", "Curiosity", "Opportunity", "Spirit"]

PHOTOS_BASE_URL = "https://api.nasa.gov/mars-photos/api/v1/rovers/"
MANIFEST_BASE_URL = "https://api.nasa.gov/mars-photos/api/v1/manifests/"

MARS_ROVERS_TRAVERSAL = ["Perseverance"]
PERSEVERANCE_TRAVERSAL_URL = "https://mars.nasa.gov/mmgis-maps/M20/Layers/json/M20_traverse.json"

MINIO_BUCKET = os.getenv('MINIO_BUCKET')

SOL_BATCH = list(range(500, 505))

MINIO_EVENTS_TOPIC = "minio-events"

PHOTOS_TABLE_NAME = "RAW_PHOTO_RESPONSE"
COORDINATES_TABLE_NAME = "RAW_COORDINATE_RESPONSE"
LOAD_COMPLETE_TOPIC = "snowflake-load-complete"

DBT_PROJECT_DIR = "/opt/airflow/dbt/martian_moments"
GOLD_TAG = "aggregate"