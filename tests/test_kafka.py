from unittest.mock import MagicMock, patch
import pytest
import json
import urllib.parse
from datetime import datetime, timezone
from src.utils.kafka import parse_kafka_message, unwrap_airflow_asset_payload, produce_kafka_message

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def mock_kafka_message():
    message = MagicMock()
    return message

@pytest.fixture
def sample_minio_events_message():
    message = MagicMock()
    message.value.return_value = json.dumps({
        "Key": "photos/mars_rover_photos_batch_sol_100.json",
        "EventName": "s3:ObjectCreated:Put"
    })
    return message

@pytest.fixture
def sample_snowflake_message():
    message = MagicMock()
    message.value.return_value = json.dumps({
        "tmp_jsonl_staging_path": "/tmp/mars_rover_photos_batch_sol_100.jsonl"
    })
    return message

@pytest.fixture
def sample_ingestion_message():
    message = MagicMock()
    message.value.return_value = json.dumps({
        "status": "success",
        "timestamp": "2025-09-22T21:47:59",
        "ingestion_schedule": [
            {"rover_name": "Curiosity", "sol_start": 100, "sol_end": 150}
        ]
    })
    return message

@pytest.fixture
def sample_airflow_events():
    event1 = MagicMock()
    event1.extra = {"payload": {"filepath": "photos/test.json", "event": "upload"}}
    
    event2 = MagicMock()
    event2.extra = {}  # No payload
    
    return [event1, event2]


# ====== PARSE_KAFKA_MESSAGE TESTS ======

def test_parse_kafka_message_minio_events(sample_minio_events_message, mock_logger):
    """Test parsing minio-events topic message"""
    topic_name = "minio-events"
    args = [None, sample_minio_events_message]
    
    result = parse_kafka_message(topic_name, args, mock_logger)
    
    assert result == "photos/mars_rover_photos_batch_sol_100.json"
    
    # Verify logging
    mock_logger.info.assert_any_call(
        f"Processing Message - Topic: {topic_name}, Payload: {{'Key': 'photos/mars_rover_photos_batch_sol_100.json', 'EventName': 's3:ObjectCreated:Put'}}"
    )
    mock_logger.info.assert_any_call(
        f"Parsed Message - Topic: {topic_name}, Upload Path: photos/mars_rover_photos_batch_sol_100.json"
    )

def test_parse_kafka_message_minio_events_url_encoded(mock_logger):
    """Test parsing minio-events with URL encoded key"""
    message = MagicMock()
    message.value.return_value = json.dumps({
        "Key": "photos/mars%20rover%20photos.json",
        "EventName": "s3:ObjectCreated:Put"
    })
    
    topic_name = "minio-events"
    args = [None, message]
    
    result = parse_kafka_message(topic_name, args, mock_logger)
    
    # Should decode URL encoding
    assert result == "photos/mars rover photos.json"

def test_parse_kafka_message_snowflake_load_complete(sample_snowflake_message, mock_logger):
    """Test parsing snowflake-load-complete topic message"""
    topic_name = "snowflake-load-complete"
    args = [None, sample_snowflake_message]
    
    result = parse_kafka_message(topic_name, args, mock_logger)
    
    assert result == "/tmp/mars_rover_photos_batch_sol_100.jsonl"
    
    # Verify logging
    mock_logger.info.assert_any_call(
        f"Processing Message - Topic: {topic_name}, Payload: {{'tmp_jsonl_staging_path': '/tmp/mars_rover_photos_batch_sol_100.jsonl'}}"
    )
    mock_logger.info.assert_any_call(
        f"Parsed Message - Topic: {topic_name}, Staging Path: /tmp/mars_rover_photos_batch_sol_100.jsonl"
    )

def test_parse_kafka_message_ingestion_scheduling(sample_ingestion_message, mock_logger):
    """Test parsing ingestion-scheduling topic message"""
    topic_name = "ingestion-scheduling"
    args = [None, sample_ingestion_message]
    
    result = parse_kafka_message(topic_name, args, mock_logger)
    
    expected_schedule = [{"rover_name": "Curiosity", "sol_start": 100, "sol_end": 150}]
    assert result == expected_schedule
    
    # Verify logging
    expected_payload = {
        "status": "success",
        "timestamp": "2025-09-22T21:47:59",
        "ingestion_schedule": [{"rover_name": "Curiosity", "sol_start": 100, "sol_end": 150}]
    }
    mock_logger.info.assert_any_call(
        f"Processing Message - Topic: {topic_name}, Payload: {expected_payload}"
    )
    mock_logger.info.assert_any_call(
        f"Parsed Message - Topic: {topic_name}, Ingestion Schedule: {expected_schedule}"
    )

def test_parse_kafka_message_unknown_topic(mock_kafka_message, mock_logger):
    """Test parsing unknown topic"""
    mock_kafka_message.value.return_value = json.dumps({"test": "data"})
    topic_name = "unknown-topic"
    args = [None, mock_kafka_message]
    
    result = parse_kafka_message(topic_name, args, mock_logger)
    
    assert result == {"test": "data"}
    
    # Verify warning logged
    mock_logger.warning.assert_called_once_with(f"Unknown topic: {topic_name}")

def test_parse_kafka_message_missing_key_minio_events(mock_logger):
    """Test parsing minio-events with missing Key"""
    message = MagicMock()
    message.value.return_value = json.dumps({
        "EventName": "s3:ObjectCreated:Put"
        # Missing 'Key'
    })
    
    topic_name = "minio-events"
    args = [None, message]
    
    result = parse_kafka_message(topic_name, args, mock_logger)
    
    # Should return empty string when Key is missing
    assert result == ""

def test_parse_kafka_message_missing_field_snowflake(mock_logger):
    """Test parsing snowflake-load-complete with missing field"""
    message = MagicMock()
    message.value.return_value = json.dumps({
        "other_field": "value"
        # Missing 'tmp_jsonl_staging_path'
    })
    
    topic_name = "snowflake-load-complete"
    args = [None, message]
    
    result = parse_kafka_message(topic_name, args, mock_logger)
    
    # Should return None when field is missing
    assert result is None

def test_parse_kafka_message_invalid_json(mock_logger):
    """Test parsing message with invalid JSON"""
    message = MagicMock()
    message.value.return_value = "invalid json"
    
    topic_name = "minio-events"
    args = [None, message]
    
    with pytest.raises(json.JSONDecodeError):
        parse_kafka_message(topic_name, args, mock_logger)


# ====== UNWRAP_AIRFLOW_ASSET_PAYLOAD TESTS ======

def test_unwrap_airflow_asset_payload_success(sample_airflow_events, mock_logger):
    """Test successful unwrapping of Airflow asset payload"""
    result = unwrap_airflow_asset_payload(sample_airflow_events, mock_logger)
    
    expected_payload = {"filepath": "photos/test.json", "event": "upload"}
    assert result == expected_payload
    
    # Verify logging
    mock_logger.info.assert_any_call(f"Unwrapping data from AssetWatcher - Events: {sample_airflow_events}")
    mock_logger.info.assert_any_call(f"Extracted data from AssetWatcher- Payload: {expected_payload}")

def test_unwrap_airflow_asset_payload_no_payload(mock_logger):
    """Test unwrapping when events have no payload"""
    event1 = MagicMock()
    event1.extra = {}  # No payload
    
    event2 = MagicMock()
    event2.extra = {"other_field": "value"}  # No payload field
    
    events = [event1, event2]
    
    result = unwrap_airflow_asset_payload(events, mock_logger)
    
    assert result is None
    
    # Verify logging
    mock_logger.info.assert_any_call(f"Unwrapping data from AssetWatcher - Events: {events}")
    mock_logger.info.assert_any_call("No payload found in AssetWatcher event")
    mock_logger.info.assert_any_call("No event to process")

def test_unwrap_airflow_asset_payload_empty_events(mock_logger):
    """Test unwrapping with empty events list"""
    events = []
    
    result = unwrap_airflow_asset_payload(events, mock_logger)
    
    assert result is None
    
    # Verify logging
    mock_logger.info.assert_any_call(f"Unwrapping data from AssetWatcher - Events: {events}")
    mock_logger.info.assert_any_call("No event to process")

def test_unwrap_airflow_asset_payload_first_event_has_payload(mock_logger):
    """Test unwrapping returns first event with payload"""
    event1 = MagicMock()
    event1.extra = {"payload": {"first": "payload"}}
    
    event2 = MagicMock()
    event2.extra = {"payload": {"second": "payload"}}
    
    events = [event1, event2]
    
    result = unwrap_airflow_asset_payload(events, mock_logger)
    
    # Should return first payload found
    assert result == {"first": "payload"}
    
    # Should not process second event
    mock_logger.info.assert_any_call("Extracted data from AssetWatcher- Payload: {'first': 'payload'}")

def test_unwrap_airflow_asset_payload_complex_payload(mock_logger):
    """Test unwrapping with complex payload data"""
    complex_payload = {
        "filepath": "coordinates/mars_rover_coordinates_2025-09-13.json",
        "event": "coordinate_upload",
        "metadata": {
            "rover": "Perseverance",
            "timestamp": "2025-09-13T15:30:00",
            "coordinate_count": 150
        }
    }
    
    event = MagicMock()
    event.extra = {"payload": complex_payload}
    
    events = [event]
    
    result = unwrap_airflow_asset_payload(events, mock_logger)
    
    assert result == complex_payload
    mock_logger.info.assert_any_call(f"Extracted data from AssetWatcher- Payload: {complex_payload}")


# ====== PRODUCE_KAFKA_MESSAGE TESTS ======

@patch('src.utils.kafka.KafkaProducer')
def test_produce_kafka_message_success(mock_kafka_producer_class, mock_logger):
    """Test successful Kafka message production"""
    # Setup mock producer instance
    mock_producer = MagicMock()
    mock_kafka_producer_class.return_value = mock_producer
    
    test_topic = "test-topic"
    test_message = {"event": "test", "data": "sample"}
    
    produce_kafka_message(test_topic, test_message, mock_logger)
    
    # Verify KafkaProducer was initialized correctly
    mock_kafka_producer_class.assert_called_once_with(
        bootstrap_servers='kafka:9092',
        value_serializer=mock_kafka_producer_class.call_args[1]['value_serializer']
    )
    
    # Test the serializer function
    serializer = mock_kafka_producer_class.call_args[1]['value_serializer']
    serialized = serializer(test_message)
    assert serialized == json.dumps(test_message).encode('utf-8')
    
    # Verify producer methods were called
    mock_producer.send.assert_called_once_with(test_topic, value=test_message)
    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once()
    
    # Verify logging
    assert mock_logger.info.call_count == 2
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Attempting to produce message - Topic:" in msg and test_topic in msg for msg in log_calls)
    assert any("Produced message - Topic:" in msg and test_topic in msg for msg in log_calls)

@patch('src.utils.kafka.KafkaProducer')
def test_produce_kafka_message_send_failure(mock_kafka_producer_class, mock_logger):
    """Test Kafka message production when send fails"""
    # Setup mock producer instance that fails on send
    mock_producer = MagicMock()
    mock_producer.send.side_effect = Exception("Send failed")
    mock_kafka_producer_class.return_value = mock_producer
    
    test_topic = "test-topic"
    test_message = {"event": "test", "data": "sample"}
    
    with pytest.raises(Exception, match="Send failed"):
        produce_kafka_message(test_topic, test_message, mock_logger)
    
    # Verify producer methods were called
    mock_producer.send.assert_called_once_with(test_topic, value=test_message)
    # flush should not be called if send fails
    mock_producer.flush.assert_not_called()
    # close should still be called in finally block
    mock_producer.close.assert_called_once()
    
    # Verify error logging
    mock_logger.error.assert_called_once()
    error_msg = mock_logger.error.call_args[0][0]
    assert "Failed to produce message" in error_msg
    assert test_topic in error_msg

@patch('src.utils.kafka.KafkaProducer')
def test_produce_kafka_message_flush_failure(mock_kafka_producer_class, mock_logger):
    """Test Kafka message production when flush fails"""
    # Setup mock producer instance that fails on flush
    mock_producer = MagicMock()
    mock_producer.flush.side_effect = Exception("Flush failed")
    mock_kafka_producer_class.return_value = mock_producer
    
    test_topic = "test-topic"
    test_message = {"event": "test", "data": "sample"}
    
    with pytest.raises(Exception, match="Flush failed"):
        produce_kafka_message(test_topic, test_message, mock_logger)
    
    # Verify producer methods were called
    mock_producer.send.assert_called_once_with(test_topic, value=test_message)
    mock_producer.flush.assert_called_once()
    # close should still be called in finally block
    mock_producer.close.assert_called_once()
    
    # Verify error logging
    mock_logger.error.assert_called_once()
    error_msg = mock_logger.error.call_args[0][0]
    assert "Failed to produce message" in error_msg
    assert test_topic in error_msg

@patch('src.utils.kafka.KafkaProducer')
def test_produce_kafka_message_complex_data(mock_kafka_producer_class, mock_logger):
    """Test Kafka message production with complex message data"""
    # Setup mock producer instance
    mock_producer = MagicMock()
    mock_kafka_producer_class.return_value = mock_producer
    
    test_topic = "complex-topic"
    complex_message = {
        "filepath": "photos/mars_rover_photos_batch_sol_100.json",
        "event": "load_complete",
        "timestamp": "2025-09-11T10:30:00",
        "metadata": {
            "size": 2048,
            "rover": "Curiosity",
            "sol": 100
        }
    }
    
    produce_kafka_message(test_topic, complex_message, mock_logger)
    
    # Verify producer was called with complex message
    mock_producer.send.assert_called_once_with(test_topic, value=complex_message)
    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once()
    
    # Test serializer with complex data
    serializer = mock_kafka_producer_class.call_args[1]['value_serializer']
    serialized = serializer(complex_message)
    expected = json.dumps(complex_message).encode('utf-8')
    assert serialized == expected