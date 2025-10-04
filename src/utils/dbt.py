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
        if result.stdout:
            logger.info(f"dbt stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"dbt stderr:\n{result.stderr}")
        return True
                    
    except subprocess.CalledProcessError as e:
        logger.error(f"Unsuccessful dbt Run - Tag: {tag}, Error: {e.returncode}")
        if e.stdout:
            logger.error(f"dbt stdout:\n{e.stdout}")
        if e.stderr:
            logger.error(f"dbt stderr:\n{e.stderr}")
        return False