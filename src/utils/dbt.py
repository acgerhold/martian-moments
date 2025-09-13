from src.config import DBT_PROJECT_DIR
import subprocess

def run_dbt_models_by_tag(tag, logger):
    logger.info(f"Attempting to run dbt models - Tag: {tag}")
    try:
        subprocess.run(
            ["dbt", "run", "--select", "tag:{tag}"],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True
        )

        logger.info(f"Ran dbt models - Tag: {tag}")
                    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running dbt models - Tag: {tag}, Error: {e.returncode}")
        logger.info(f"stdout: {e.stdout}")
        logger.info(f"stderr: {e.stderr}")
        raise