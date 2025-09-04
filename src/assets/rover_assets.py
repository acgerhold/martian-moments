from airflow.assets import Asset, KafkaAssetWatcher

MARS_ROVER_PHOTOS = Asset("mars://photos/rover_data")

mars_photos_watcher = KafkaAssetWatcher(
    asset=MARS_ROVER_PHOTOS,
    kafka_config={
        "bootstrap_servers": "kafka:9092",
        "group_id": "airflow_mars_photos_consumer",
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "value_deserializer": lambda m: m.decode('utf-8')
    },
    topic="minio-bucket-notifications",
    event_filter=lambda event: (
        event.get("eventName", "").startswith("s3:ObjectCreated:") and
        event.get("s3", {}).get("object", {}).get("key", "").startswith("photos/") and
        event.get("s3", {}).get("object", {}).get("key", "").endswith(".json")
    )
)