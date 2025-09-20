from src.config import DBT_PROJECT_DIR
import subprocess
import time

def run_dbt_models_by_tag(tag, logger):
    logger.info(f"Attempting dbt Run - Tag: {tag}")
    try:
        result = subprocess.run(
            ["dbt", "run", "--select", f"tag:{tag}"],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True
        )

        if result.returncode != 0:
            raise Exception(f"Failed dbt Run - Tag: {tag}, Error: {result.stderr}")

        logger.info(f"Succesful dbt Run - Tag: {tag}")
        return f"{tag}: Success"
                    
    except subprocess.CalledProcessError as e:
        logger.error(f"Unsuccessful dbt Run - Tag: {tag}, Error: {e.returncode}")
        raise