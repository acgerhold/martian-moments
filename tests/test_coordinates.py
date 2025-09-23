from unittest.mock import patch, MagicMock
import pytest
import json
from src.ingestion.coordinates import extract_coordinates_from_nasa, create_final_coordinates_json, generate_tasks_for_coordinates_batch

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def sample_coordinate_response():
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [77.31423643, 18.4903187, -2350.791749],
                        [77.3142313, 18.49031325, -2350.709651],
                        [77.31423054, 18.49031246, -2350.692779]
                    ]
                },
                "properties": {
                    "sol": 52,
                    "fromRMC": "52_2266",
                    "toRMC": "52_2480",
                    "length": 33.82,
                    "SCLK_START": 768850261,
                    "SCLK_END": 768854392
                }
            }
        ]
    }

@pytest.fixture
def sample_enhanced_coordinate_response(sample_coordinate_response):
    return {
        "rover": "Perseverance",
        "coordinate_response": sample_coordinate_response
    }

# ====== EXTRACT_COORDINATES_FROM_NASA TESTS ======

@patch('src.ingestion.coordinates.requests.get')
def test_extract_coordinates_from_nasa_success_perseverance(mock_get, mock_logger, sample_coordinate_response):
    """Test successful coordinate extraction for Perseverance"""
    mock_response = MagicMock()
    mock_response.json.return_value = sample_coordinate_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extract_coordinates_from_nasa("Perseverance", mock_logger)
    
    assert result["rover"] == "Perseverance"
    assert result["coordinate_response"] == sample_coordinate_response
    mock_logger.info.assert_any_call("Processing Coordinates Request - Rover: Perseverance")
    mock_logger.info.assert_any_call("Successful Coordinates Request - Rover: Perseverance, Coordinate Count: 1")

@patch('src.ingestion.coordinates.requests.get')
def test_extract_coordinates_from_nasa_request_timeout(mock_get, mock_logger):
    """Test coordinate extraction with request timeout"""
    mock_get.side_effect = Exception("Timeout error")
    
    result = extract_coordinates_from_nasa("Perseverance", mock_logger)
    
    assert result == {"features": []}
    mock_logger.error.assert_called_once_with("Unsuccessful Coordinates Request - Rover: Perseverance, Error: Timeout error")

@patch('src.ingestion.coordinates.requests.get')
def test_extract_coordinates_from_nasa_empty_response(mock_get, mock_logger):
    """Test coordinate extraction with empty features"""
    mock_response = MagicMock()
    mock_response.json.return_value = {"features": []}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extract_coordinates_from_nasa("Perseverance", mock_logger)
    
    assert result["rover"] == "Perseverance"
    assert result["coordinate_response"]["features"] == []
    mock_logger.info.assert_any_call("Successful Coordinates Request - Rover: Perseverance, Coordinate Count: 0")

# ====== CREATE_FINAL_COORDINATES_JSON TESTS ======

def test_create_final_coordinates_json_success(mock_logger, sample_enhanced_coordinate_response):
    """Test successful creation of final coordinates JSON"""
    all_rover_coordinate_results = [sample_enhanced_coordinate_response]
    
    with patch('src.ingestion.coordinates.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-13T15:30:00"
        
        result = create_final_coordinates_json(all_rover_coordinate_results, mock_logger)
        
        assert result["filename"] == "mars_rover_coordinates_2025-09-13T15:30:00.json"
        assert result["coordinate_count"] == 1
        assert result["ingestion_date"] == "2025-09-13T15:30:00"
        assert len(result["coordinates"]) == 1
        assert result["coordinates"][0]["rover_name"] == "Perseverance"
        
        mock_logger.info.assert_any_call("Creating Coordinates Batch File")
        mock_logger.info.assert_any_call("Created Coordinates Batch - File: mars_rover_coordinates_2025-09-13T15:30:00.json, Coordinate Count: 1")

def test_create_final_coordinates_json_multiple_rovers(mock_logger):
    """Test creation of final coordinates JSON with multiple rovers"""
    rover1_response = {
        "rover": "Perseverance",
        "coordinate_response": {
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"coordinates": [[77.314, 18.490, -2350.79]]},
                    "properties": {"sol": 52}
                }
            ]
        }
    }
    rover2_response = {
        "rover": "Curiosity",
        "coordinate_response": {
            "features": [
                {
                    "type": "Feature", 
                    "geometry": {"coordinates": [[88.123, 19.456, -1234.56]]},
                    "properties": {"sol": 100}
                }
            ]
        }
    }
    
    all_rover_coordinate_results = [rover1_response, rover2_response]
    
    with patch('src.ingestion.coordinates.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-13T15:30:00"
        
        result = create_final_coordinates_json(all_rover_coordinate_results, mock_logger)
        
        assert result["coordinate_count"] == 2
        assert result["coordinates"][0]["rover_name"] == "Perseverance"
        assert result["coordinates"][1]["rover_name"] == "Curiosity"

def test_create_final_coordinates_json_empty_coordinates(mock_logger):
    """Test creation of final coordinates JSON with empty coordinate results"""
    rover_response = {
        "rover": "Perseverance",
        "coordinate_response": {"features": []}
    }
    
    all_rover_coordinate_results = [rover_response]
    
    with patch('src.ingestion.coordinates.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-13T15:30:00"
        
        result = create_final_coordinates_json(all_rover_coordinate_results, mock_logger)
        
        assert result["coordinate_count"] == 0
        assert result["coordinates"] == []

def test_create_final_coordinates_json_missing_rover_metadata(mock_logger):
    """Test handling of coordinate results missing rover metadata"""
    # Simulate old format without rover metadata
    old_format_response = {
        "coordinate_response": {
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"coordinates": [[77.314, 18.490, -2350.79]]},
                    "properties": {"sol": 52}
                }
            ]
        }
        # Missing 'rover' key
    }
    
    all_rover_coordinate_results = [old_format_response]
    
    with patch('src.ingestion.coordinates.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-13T15:30:00"
        
        result = create_final_coordinates_json(all_rover_coordinate_results, mock_logger)
        
        assert result["coordinate_count"] == 1
        assert result["coordinates"][0]["rover_name"] is None

# ====== GENERATE_TASKS_FOR_COORDINATES_BATCH TESTS ======

def test_generate_tasks_for_coordinates_batch_single_rover(mock_logger):
    """Test task generation for single rover"""
    rovers = ["Perseverance"]
    
    result = generate_tasks_for_coordinates_batch(rovers, mock_logger)
    
    assert len(result) == 1
    assert result[0] == {"rover": "Perseverance"}
    mock_logger.info.assert_any_call("Generating Tasks for Coordinates DAG")
    mock_logger.info.assert_any_call("1 Tasks Generated")

def test_generate_tasks_for_coordinates_batch_multiple_rovers(mock_logger):
    """Test task generation for multiple rovers"""
    rovers = ["Perseverance", "Curiosity", "Opportunity"]
    
    result = generate_tasks_for_coordinates_batch(rovers, mock_logger)
    
    assert len(result) == 3
    assert result[0] == {"rover": "Perseverance"}
    assert result[1] == {"rover": "Curiosity"}
    assert result[2] == {"rover": "Opportunity"}
    mock_logger.info.assert_any_call("3 Tasks Generated")

def test_generate_tasks_for_coordinates_batch_empty_rovers(mock_logger):
    """Test task generation with empty rover list"""
    rovers = []
    
    result = generate_tasks_for_coordinates_batch(rovers, mock_logger)
    
    assert len(result) == 0
    mock_logger.info.assert_any_call("0 Tasks Generated")