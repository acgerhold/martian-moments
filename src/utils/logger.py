import os
import logging
from datetime import datetime

def setup_logger_advanced(name: str, 
                         log_file: str = "pipeline.log", 
                         category_dir: str = None,
                         use_date_subdirectory: bool = True,
                         date_format: str = '%Y-%m-%d'):
    """
    Logger setup with explicit category directories.
    
    Args:
        name: Logger name 
        log_file: Log filename (just the filename, not full path)
        category_dir: Category directory (e.g., 'data_processing', 'api', 'ml')
        use_date_subdirectory: If True, adds date subdirectory
        date_format: Date format for subdirectory (default: YYYY-MM-DD)
    
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    
    path_parts = ["logs"]
    
    if category_dir:
        path_parts.append(category_dir)
    
    if use_date_subdirectory:
        date_str = datetime.now().strftime(date_format)
        path_parts.append(date_str)
    
    path_parts.append(log_file)
    
    log_file_path = os.path.join(project_root, *path_parts)

    log_directory = os.path.dirname(log_file_path)
    os.makedirs(log_directory, exist_ok=True)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger