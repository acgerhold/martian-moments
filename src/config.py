from dotenv import load_dotenv
import os

load_dotenv()

NASA_KEY = os.getenv('NASA_KEY')

MARS_ROVERS = ["Perseverance", "Curiosity", "Opportunity", "Spirit"]
MARS_ROVERS_TRAVERSAL = ["Perseverance"]

PHOTOS_BASE_URL = "https://api.nasa.gov/mars-photos/api/v1/rovers/"
MANIFEST_BASE_URL = "https://api.nasa.gov/mars-photos/api/v1/manifests/"

PERSEVERANCE_TRAVERSAL_URL = "https://mars.nasa.gov/mmgis-maps/M20/Layers/json/M20_traverse.json"

MINIO_BUCKET = os.getenv('MINIO_BUCKET')

MINIO_EVENTS_TOPIC = "minio-events"
LOAD_COMPLETE_TOPIC = "snowflake-load-complete"
INGESTION_SCHEDULING_TOPIC = "ingestion-scheduling"

DBT_PROJECT_DIR = "/opt/airflow/dbt/martian_moments"
PHOTOS_TABLE_NAME = "RAW_PHOTO_RESPONSE"
COORDINATES_TABLE_NAME = "RAW_COORDINATE_RESPONSE"
MANIFESTS_TABLE_NAME = "RAW_MANIFEST_RESPONSE"

GOLD_TAG = "aggregate"

INGESTION_PLANNING_VIEW = "INGESTION_PLANNING"

