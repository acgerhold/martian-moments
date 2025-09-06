from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import dag, task, Asset, AssetWatcher
import json
import urllib.parse

def apply_function(*args, **kwargs):
    message = args[-1]
    try:
        val = json.loads(message.value())
        key = urllib.parse.unquote(val.get('Key', ''))
        print(f"File uploaded: {key}")
        return {"filepath": key, "event": val}
    except Exception as e:
        print(f"Error parsing message: {e}")
        return {"error": str(e)}

trigger = MessageQueueTrigger(
    queue="kafka://kafka:9092/minio-events",
    apply_function="snowflake_load_dag.apply_function"
)

kafka_topic_asset = Asset(
    name="kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)]
)

@dag(schedule=[kafka_topic_asset])
def load_photos_to_snowflake_dag():
    
    @task
    def process_message(triggering_asset_events=None):
        for event in triggering_asset_events[kafka_topic_asset]:
            print(f"Processing message: {event}")

    process_message()

load_photos_to_snowflake_dag()