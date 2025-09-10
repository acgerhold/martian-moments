from unittest.mock import MagicMock
import pytest
import json
import urllib.parse
from src.utils.kafka import parse_message, extract_filepath_from_message

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
    assert any("Message:" in msg for msg in log_calls)
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
    assert any("Message:" in msg for msg in log_calls)
    assert any("Filepath extracted" in msg and test_filepath in msg for msg in log_calls)
