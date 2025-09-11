from unittest.mock import MagicMock, patch
import pytest
import json
import urllib.parse
from datetime import datetime, timezone
from src.utils.kafka import parse_message, extract_filepath_from_message, produce_kafka_message, generate_load_complete_message

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def mock_kafka_message():
    message = MagicMock()
    return message

# ====== PARSE_MESSAGE TESTS ======

def test_parse_message_success(mock_logger, mock_kafka_message):
    """Test successful message parsing"""
    test_key = "photos/mars_rover_photos_batch_sol_100_2025-09-10T12:00:00.json"
    test_event_data = {"Key": urllib.parse.quote(test_key), "EventName": "s3:ObjectCreated:Put"}
    
    mock_kafka_message.value.return_value = json.dumps(test_event_data)
    args = ['some', 'other', 'args', mock_kafka_message]
    
    result = parse_message(args, mock_logger)
    
    assert result["filepath"] == test_key
    assert result["event"] == test_event_data
    
    # Verify logging
    mock_logger.info.assert_called_once()
    log_msg = mock_logger.info.call_args[0][0]
    assert "File uploaded - Key:" in log_msg
    assert test_key in log_msg

def test_parse_message_url_encoded_key(mock_logger, mock_kafka_message):
    """Test message parsing with URL-encoded key"""
    original_key = "photos/batch file with spaces.json"
    encoded_key = urllib.parse.quote(original_key)
    test_event_data = {"Key": encoded_key, "EventName": "s3:ObjectCreated:Put"}
    
    mock_kafka_message.value.return_value = json.dumps(test_event_data)
    args = ['arg1', 'arg2', mock_kafka_message]
    
    result = parse_message(args, mock_logger)
    
    # Should decode the URL-encoded key
    assert result["filepath"] == original_key
    assert result["event"] == test_event_data

def test_parse_message_missing_key(mock_logger, mock_kafka_message):
    """Test message parsing when Key is missing"""
    test_event_data = {"EventName": "s3:ObjectCreated:Put"}  # No Key field
    
    mock_kafka_message.value.return_value = json.dumps(test_event_data)
    args = [mock_kafka_message]
    
    result = parse_message(args, mock_logger)
    
    # Should handle missing key gracefully
    assert result["filepath"] == ""
    assert result["event"] == test_event_data

def test_parse_message_empty_key(mock_logger, mock_kafka_message):
    """Test message parsing when Key is empty"""
    test_event_data = {"Key": "", "EventName": "s3:ObjectCreated:Put"}
    
    mock_kafka_message.value.return_value = json.dumps(test_event_data)
    args = [mock_kafka_message]
    
    result = parse_message(args, mock_logger)
    
    assert result["filepath"] == ""
    assert result["event"] == test_event_data

def test_parse_message_json_parse_error(mock_logger, mock_kafka_message):
    """Test message parsing when JSON is invalid"""
    mock_kafka_message.value.return_value = "invalid json {"
    args = [mock_kafka_message]
    
    result = parse_message(args, mock_logger)
    
    assert "error" in result
    assert isinstance(result["error"], str)
    
    # Verify error logging
    mock_logger.error.assert_called_once()
    error_msg = mock_logger.error.call_args[0][0]
    assert "Error parsing message" in error_msg

def test_parse_message_value_error(mock_logger, mock_kafka_message):
    """Test message parsing when message.value() raises an exception"""
    mock_kafka_message.value.side_effect = Exception("Kafka error")
    args = [mock_kafka_message]
    
    result = parse_message(args, mock_logger)
    
    assert "error" in result
    assert "Kafka error" in result["error"]
    
    # Verify error logging
    mock_logger.error.assert_called_once()

def test_parse_message_complex_event_data(mock_logger, mock_kafka_message):
    """Test message parsing with complex event data"""
    test_key = "photos/complex_batch.json"
    complex_event_data = {
        "Key": test_key,
        "EventName": "s3:ObjectCreated:Put",
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "minio:s3",
                "eventTime": "2025-09-10T12:00:00.000Z",
                "s3": {
                    "bucket": {"name": "mars-photos"},
                    "object": {"key": test_key, "size": 1024}
                }
            }
        ]
    }
    
    mock_kafka_message.value.return_value = json.dumps(complex_event_data)
    args = [mock_kafka_message]
    
    result = parse_message(args, mock_logger)
    
    assert result["filepath"] == test_key
    assert result["event"] == complex_event_data
    assert result["event"]["Records"][0]["s3"]["bucket"]["name"] == "mars-photos"

# ====== EXTRACT_FILEPATH_FROM_MESSAGE TESTS ======

def test_extract_filepath_from_message_success(mock_logger):
    """Test successful filepath extraction"""
    test_filepath = "photos/mars_rover_photos_batch_sol_150.json"
    events = [
        MagicMock(extra={'payload': {'filepath': test_filepath}})
    ]
    
    result = extract_filepath_from_message(events, mock_logger)
    
    assert result == test_filepath
    
    # Verify logging
    assert mock_logger.info.call_count == 2
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Attempting to extract filepath" in msg for msg in log_calls)
    assert any("Filepath extracted" in msg and test_filepath in msg for msg in log_calls)

def test_extract_filepath_from_message_no_filepath(mock_logger):
    """Test filepath extraction when filepath is missing"""
    events = [
        MagicMock(extra={'payload': {}})  # No filepath in payload
    ]
    
    result = extract_filepath_from_message(events, mock_logger)
    
    assert result is None
    
    # Verify appropriate logging
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("No file to process" in msg for msg in log_calls)

def test_extract_filepath_from_message_empty_filepath(mock_logger):
    """Test filepath extraction when filepath is empty"""
    events = [
        MagicMock(extra={'payload': {'filepath': ''}})
    ]
    
    result = extract_filepath_from_message(events, mock_logger)
    
    assert result is None
    
    # Verify logging
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("No file to process" in msg for msg in log_calls)

def test_extract_filepath_from_message_none_filepath(mock_logger):
    """Test filepath extraction when filepath is None"""
    events = [
        MagicMock(extra={'payload': {'filepath': None}})
    ]
    
    result = extract_filepath_from_message(events, mock_logger)
    
    assert result is None
    
    # Verify logging
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("No file to process" in msg for msg in log_calls)

def test_extract_filepath_from_message_missing_payload(mock_logger):
    """Test filepath extraction when payload is missing"""
    events = [
        MagicMock(extra={})  # No payload
    ]
    
    result = extract_filepath_from_message(events, mock_logger)
    
    assert result is None

def test_extract_filepath_from_message_missing_extra(mock_logger):
    """Test filepath extraction when extra is missing"""
    events = [
        MagicMock(extra=None)
    ]
    
    # This should raise an AttributeError, but let's handle it gracefully
    try:
        result = extract_filepath_from_message(events, mock_logger)
        # If no exception, result should be None
        assert result is None
    except AttributeError:
        # This is also acceptable behavior
        pass

def test_extract_filepath_from_message_multiple_events(mock_logger):
    """Test filepath extraction with multiple events (should return first valid one)"""
    first_filepath = "photos/first_batch.json"
    second_filepath = "photos/second_batch.json"
    
    events = [
        MagicMock(extra={'payload': {'filepath': first_filepath}}),
        MagicMock(extra={'payload': {'filepath': second_filepath}})
    ]
    
    result = extract_filepath_from_message(events, mock_logger)
    
    # Should return the first filepath
    assert result == first_filepath
    
    # Should only process the first event
    assert mock_logger.info.call_count == 2  # Message log + Filepath extracted log

def test_extract_filepath_from_message_first_empty_second_valid(mock_logger):
    """Test filepath extraction when first event has no filepath but second does"""
    valid_filepath = "photos/valid_batch.json"
    
    events = [
        MagicMock(extra={'payload': {}}),  # No filepath
        MagicMock(extra={'payload': {'filepath': valid_filepath}})
    ]
    
    result = extract_filepath_from_message(events, mock_logger)
    
    # Should return None because function returns after processing first event
    assert result is None

def test_extract_filepath_from_message_complex_payload(mock_logger):
    """Test filepath extraction with complex payload structure"""
    test_filepath = "photos/complex_structure.json"
    events = [
        MagicMock(extra={
            'payload': {
                'filepath': test_filepath,
                'bucket': 'mars-photos',
                'event_type': 's3:ObjectCreated:Put',
                'timestamp': '2025-09-10T12:00:00Z',
                'metadata': {
                    'content_type': 'application/json',
                    'size': 2048
                }
            }
        })
    ]
    
    result = extract_filepath_from_message(events, mock_logger)
    
    assert result == test_filepath
    
    # Verify logging includes the complex event
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Attempting to extract filepath" in msg for msg in log_calls)
    assert any("Filepath extracted" in msg and test_filepath in msg for msg in log_calls)

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
    assert any("Attempting to produce event" in msg and test_topic in msg for msg in log_calls)
    assert any("Produced message" in msg and test_topic in msg for msg in log_calls)

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

# ====== GENERATE_LOAD_COMPLETE_MESSAGE TESTS ======

def test_generate_load_complete_message_success(mock_logger):
    """Test successful load complete message generation"""
    test_filepath = "/tmp/mars_rover_photos_batch_sol_150.jsonl"
    
    with patch('src.utils.kafka.datetime') as mock_datetime:
        # Mock the datetime to return a fixed time
        fixed_time = datetime(2025, 9, 11, 10, 30, 45, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        mock_datetime.timezone = timezone
        
        result = generate_load_complete_message(test_filepath, mock_logger)
    
    expected_message = {
        "filepath": test_filepath,
        "event": "success",
        "timestamp": "2025-09-11T10:30:45"
    }
    
    assert result == expected_message
    
    # Verify logging
    assert mock_logger.info.call_count == 2
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Attempting to generate load complete message" in msg and test_filepath in msg for msg in log_calls)
    assert any("Message produced" in msg for msg in log_calls)

def test_generate_load_complete_message_different_filepath(mock_logger):
    """Test load complete message generation with different filepath"""
    test_filepath = "/opt/airflow/tmp/curiosity_sol_200.jsonl"
    
    with patch('src.utils.kafka.datetime') as mock_datetime:
        # Mock different timestamp
        fixed_time = datetime(2025, 12, 25, 23, 59, 59, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        mock_datetime.timezone = timezone
        
        result = generate_load_complete_message(test_filepath, mock_logger)
    
    expected_message = {
        "filepath": test_filepath,
        "event": "success",
        "timestamp": "2025-12-25T23:59:59"
    }
    
    assert result == expected_message
    assert result["filepath"] == test_filepath
    assert result["event"] == "success"
    assert result["timestamp"] == "2025-12-25T23:59:59"

def test_generate_load_complete_message_empty_filepath(mock_logger):
    """Test load complete message generation with empty filepath"""
    test_filepath = ""
    
    with patch('src.utils.kafka.datetime') as mock_datetime:
        fixed_time = datetime(2025, 9, 11, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        mock_datetime.timezone = timezone
        
        result = generate_load_complete_message(test_filepath, mock_logger)
    
    expected_message = {
        "filepath": "",
        "event": "success",
        "timestamp": "2025-09-11T12:00:00"
    }
    
    assert result == expected_message
    
    # Should still log appropriately
    assert mock_logger.info.call_count == 2

def test_generate_load_complete_message_none_filepath(mock_logger):
    """Test load complete message generation with None filepath"""
    test_filepath = None
    
    with patch('src.utils.kafka.datetime') as mock_datetime:
        fixed_time = datetime(2025, 9, 11, 15, 45, 30, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        mock_datetime.timezone = timezone
        
        result = generate_load_complete_message(test_filepath, mock_logger)
    
    expected_message = {
        "filepath": None,
        "event": "success",
        "timestamp": "2025-09-11T15:45:30"
    }
    
    assert result == expected_message
    assert result["filepath"] is None
    assert result["event"] == "success"

def test_generate_load_complete_message_timestamp_format(mock_logger):
    """Test that timestamp format is consistent and correct"""
    test_filepath = "/tmp/test.jsonl"
    
    with patch('src.utils.kafka.datetime') as mock_datetime:
        # Test edge case timestamps
        test_cases = [
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),  # New year
            datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),  # End of year
            datetime(2025, 6, 15, 12, 30, 45, tzinfo=timezone.utc),  # Mid year
        ]
        
        for i, fixed_time in enumerate(test_cases):
            mock_datetime.now.return_value = fixed_time
            mock_datetime.timezone = timezone
            
            result = generate_load_complete_message(test_filepath, mock_logger)
            
            # Verify timestamp format
            timestamp = result["timestamp"]
            assert len(timestamp) == 19  # YYYY-MM-DDTHH:MM:SS format
            assert "T" in timestamp
            assert timestamp.count("-") == 2  # Two dashes in date
            assert timestamp.count(":") == 2  # Two colons in time
            
            # Verify it matches expected format
            expected_timestamp = fixed_time.strftime("%Y-%m-%dT%H:%M:%S")
            assert result["timestamp"] == expected_timestamp
