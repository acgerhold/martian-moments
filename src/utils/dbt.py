from src.config import DBT_PROJECT_DIR
import subprocess

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

        logger.info(f"Succesful dbt Run - Tag: {tag}")
        return True
                    
    except subprocess.CalledProcessError as e:
        logger.error(f"Unsuccessful dbt Run - Tag: {tag}, Error: {e.returncode}")
        return False