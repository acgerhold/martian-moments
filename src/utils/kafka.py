import json
import urllib.parse

def parse_message(args, logger):
    message = args[-1]
    try:
        val = json.loads(message.value())
        key = urllib.parse.unquote(val.get('Key', ''))

        logger.info(f"File uploaded - Key: {key}")
        return {"filepath": key, "event": val}
    except Exception as e:
        logger.error(f"Error parsing message - Error: {e}")
        return {"error": str(e)}
    
def extract_filepath_from_message(events, logger):
    for event in events:
        logger.info(f"Message: {event}")

        minio_filepath = event.extra.get('payload', {}).get('filepath')
        logger.info(f"Filepath extracted - Path: {minio_filepath}")

        if not minio_filepath:
            logger.info("No file to process")
            return None
        
        return minio_filepath