from dotenv import load_dotenv
import os

load_dotenv()

NASA_KEY = os.getenv('NASA_KEY')

MARS_ROVERS = ["Perseverance", "Curiosity", "Opportunity", "Spirit"]

PHOTOS_BASE_URL = "https://api.nasa.gov/mars-photos/api/v1/rovers/"
MANIFEST_BASE_URL = "https://api.nasa.gov/mars-photos/api/v1/manifests/"

MINIO_BUCKET = os.getenv('MINIO_BUCKET')

BATCH_1 = list(range(0,250))
BATCH_2 = list(range(250, 300))
BATCH_3 = list(range(370, 375))

DBT_PROJECT_DIR = "/opt/airflow/dbt/martian_moments"