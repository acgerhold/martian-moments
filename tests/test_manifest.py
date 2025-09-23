from unittest.mock import patch, MagicMock
import pytest
import json

from src.ingestion.manifest import extract_manifests_from_nasa, create_final_manifest_json, generate_tasks_for_manifest_batch

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def sample_manifest_response():
    return {
        "photo_manifest": {
            "name": "Curiosity",
            "landing_date": "2012-08-05",
            "launch_date": "2011-11-26",
            "status": "active",
            "max_sol": 4000,
            "max_date": "2023-10-15",
            "total_photos": 695000,
            "photos": [
                {
                    "sol": 1,
                    "earth_date": "2012-08-06",
                    "total_photos": 16,
                    "cameras": ["CHEMCAM", "FHAZ", "MARDI", "RHAZ"]
                },
                {
                    "sol": 2,
                    "earth_date": "2012-08-07", 
                    "total_photos": 74,
                    "cameras": ["FHAZ", "HAZCAM", "MAST", "RHAZ"]
                }
            ]
        }
    }

@pytest.fixture 
def sample_empty_manifest_response():
    return {"photo_manifest": {}}

@pytest.fixture
def sample_multiple_manifest_results():
    return [
        {
            "photo_manifest": {
                "name": "Curiosity",
                "max_sol": 4000,
                "total_photos": 695000
            }
        },
        {
            "photo_manifest": {
                "name": "Perseverance", 
                "max_sol": 1000,
                "total_photos": 250000
            }
        }
    ]


# ====== EXTRACT_MANIFESTS_FROM_NASA TESTS ======

@patch('src.ingestion.manifest.requests.get')
def test_extract_manifests_from_nasa_success(mock_get, mock_logger, sample_manifest_response):
    """Test successful manifest extraction"""
    mock_response = MagicMock()
    mock_response.json.return_value = sample_manifest_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extract_manifests_from_nasa("Curiosity", mock_logger)
    
    assert result == sample_manifest_response
    mock_logger.info.assert_any_call("Processing Manifest Request - Rover: Curiosity")
    mock_logger.info.assert_any_call("Successful Manifest Request - Rover: Curiosity")

@patch('src.ingestion.manifest.requests.get')
def test_extract_manifests_from_nasa_request_timeout(mock_get, mock_logger):
    """Test manifest extraction with request timeout"""
    mock_get.side_effect = Exception("Timeout error")
    
    result = extract_manifests_from_nasa("Spirit", mock_logger)
    
    assert result == {"photo_manifest": []}
    mock_logger.error.assert_called_once_with("Unsuccessful Manifest Request - Rover: Spirit, Error: Timeout error")

@patch('src.ingestion.manifest.requests.get')
def test_extract_manifests_from_nasa_http_error(mock_get, mock_logger):
    """Test manifest extraction with HTTP error"""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("HTTP 404 Error")
    mock_get.return_value = mock_response
    
    result = extract_manifests_from_nasa("InvalidRover", mock_logger)
    
    assert result == {"photo_manifest": []}
    mock_logger.error.assert_called_once_with("Unsuccessful Manifest Request - Rover: InvalidRover, Error: HTTP 404 Error")

@patch('src.ingestion.manifest.requests.get')
def test_extract_manifests_from_nasa_empty_response(mock_get, mock_logger, sample_empty_manifest_response):
    """Test manifest extraction with empty manifest"""
    mock_response = MagicMock()
    mock_response.json.return_value = sample_empty_manifest_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extract_manifests_from_nasa("Opportunity", mock_logger)
    
    assert result == sample_empty_manifest_response
    mock_logger.info.assert_any_call("Successful Manifest Request - Rover: Opportunity")

@patch('src.ingestion.manifest.requests.get')
def test_extract_manifests_from_nasa_json_decode_error(mock_get, mock_logger):
    """Test manifest extraction with malformed JSON response"""
    mock_response = MagicMock()
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extract_manifests_from_nasa("Curiosity", mock_logger)
    
    assert result == {"photo_manifest": []}
    mock_logger.error.assert_called_once_with("Unsuccessful Manifest Request - Rover: Curiosity, Error: Invalid JSON")

@patch('src.ingestion.manifest.requests.get')
def test_extract_manifests_from_nasa_different_rovers(mock_get, mock_logger):
    """Test manifest extraction for different rover names"""
    rovers = ["Curiosity", "Perseverance", "Opportunity", "Spirit"]
    
    for rover in rovers:
        mock_response = MagicMock()
        mock_response.json.return_value = {"photo_manifest": {"name": rover}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = extract_manifests_from_nasa(rover, mock_logger)
        
        assert result["photo_manifest"]["name"] == rover
        mock_logger.info.assert_any_call(f"Processing Manifest Request - Rover: {rover}")
        mock_logger.info.assert_any_call(f"Successful Manifest Request - Rover: {rover}")


# ====== CREATE_FINAL_MANIFEST_JSON TESTS ======

def test_create_final_manifest_json_success(mock_logger, sample_multiple_manifest_results):
    """Test successful creation of final manifest JSON"""
    with patch('src.ingestion.manifest.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-23T15:30:00"
        
        result = create_final_manifest_json(sample_multiple_manifest_results, mock_logger)
        
        expected_manifests = [
            {"name": "Curiosity", "max_sol": 4000, "total_photos": 695000},
            {"name": "Perseverance", "max_sol": 1000, "total_photos": 250000}
        ]
        
        assert result["filename"] == "mars_rover_manifests_2025-09-23T15:30:00.json"
        assert result["manifests"] == expected_manifests
        assert result["ingestion_date"] == "2025-09-23T15:30:00"
        assert len(result["manifests"]) == 2
        
        mock_logger.info.assert_any_call("Creating Manifest Batch File")
        mock_logger.info.assert_any_call("Created Manifest Batch - File: mars_rover_manifests_2025-09-23T15:30:00.json")

def test_create_final_manifest_json_single_manifest(mock_logger):
    """Test creation of final manifest JSON with single manifest"""
    single_manifest_result = [
        {
            "photo_manifest": {
                "name": "Curiosity",
                "max_sol": 4000,
                "status": "active"
            }
        }
    ]
    
    with patch('src.ingestion.manifest.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-23T15:30:00"
        
        result = create_final_manifest_json(single_manifest_result, mock_logger)
        
        assert len(result["manifests"]) == 1
        assert result["manifests"][0]["name"] == "Curiosity"

def test_create_final_manifest_json_empty_manifests(mock_logger):
    """Test creation of final manifest JSON with empty manifests"""
    empty_manifest_results = [
        {"photo_manifest": {}},
        {"photo_manifest": {}}
    ]
    
    with patch('src.ingestion.manifest.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-23T15:30:00"
        
        result = create_final_manifest_json(empty_manifest_results, mock_logger)
        
        assert result["manifests"] == []
        assert result["filename"] == "mars_rover_manifests_2025-09-23T15:30:00.json"

def test_create_final_manifest_json_missing_photo_manifest_key(mock_logger):
    """Test creation with missing photo_manifest key"""
    invalid_manifest_results = [
        {"some_other_key": "value"},
        {"photo_manifest": {"name": "Perseverance"}}
    ]
    
    with patch('src.ingestion.manifest.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-23T15:30:00"
        
        result = create_final_manifest_json(invalid_manifest_results, mock_logger)
        
        # Should only include the valid manifest
        assert len(result["manifests"]) == 1
        assert result["manifests"][0]["name"] == "Perseverance"

def test_create_final_manifest_json_complex_manifest_data(mock_logger):
    """Test creation with complex manifest data structure"""
    complex_manifest_results = [
        {
            "photo_manifest": {
                "name": "Curiosity",
                "landing_date": "2012-08-05",
                "launch_date": "2011-11-26",
                "status": "active",
                "max_sol": 4000,
                "max_date": "2023-10-15",
                "total_photos": 695000,
                "photos": [
                    {"sol": 1, "earth_date": "2012-08-06", "total_photos": 16},
                    {"sol": 2, "earth_date": "2012-08-07", "total_photos": 74}
                ]
            }
        }
    ]
    
    with patch('src.ingestion.manifest.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-23T15:30:00"
        
        result = create_final_manifest_json(complex_manifest_results, mock_logger)
        
        manifest = result["manifests"][0]
        assert manifest["name"] == "Curiosity"
        assert manifest["total_photos"] == 695000
        assert len(manifest["photos"]) == 2
        assert manifest["photos"][0]["sol"] == 1


# ====== GENERATE_TASKS_FOR_MANIFEST_BATCH TESTS ======

def test_generate_tasks_for_manifest_batch_single_rover(mock_logger):
    """Test task generation for single rover"""
    rovers = ["Curiosity"]
    
    result = generate_tasks_for_manifest_batch(rovers, mock_logger)
    
    expected_tasks = [{"rover": "Curiosity"}]
    assert result == expected_tasks
    
    mock_logger.info.assert_any_call("Generating Tasks for Manifest DAG")
    mock_logger.info.assert_any_call("1 Tasks Generated")

def test_generate_tasks_for_manifest_batch_multiple_rovers(mock_logger):
    """Test task generation for multiple rovers"""
    rovers = ["Curiosity", "Perseverance", "Opportunity", "Spirit"]
    
    result = generate_tasks_for_manifest_batch(rovers, mock_logger)
    
    expected_tasks = [
        {"rover": "Curiosity"},
        {"rover": "Perseverance"},
        {"rover": "Opportunity"},
        {"rover": "Spirit"}
    ]
    assert result == expected_tasks
    assert len(result) == 4
    
    mock_logger.info.assert_any_call("Generating Tasks for Manifest DAG")
    mock_logger.info.assert_any_call("4 Tasks Generated")

def test_generate_tasks_for_manifest_batch_empty_rovers(mock_logger):
    """Test task generation with empty rover list"""
    rovers = []
    
    result = generate_tasks_for_manifest_batch(rovers, mock_logger)
    
    assert result == []
    mock_logger.info.assert_any_call("Generating Tasks for Manifest DAG")
    mock_logger.info.assert_any_call("0 Tasks Generated")

def test_generate_tasks_for_manifest_batch_duplicate_rovers(mock_logger):
    """Test task generation with duplicate rover names"""
    rovers = ["Curiosity", "Curiosity", "Perseverance"]
    
    result = generate_tasks_for_manifest_batch(rovers, mock_logger)
    
    expected_tasks = [
        {"rover": "Curiosity"},
        {"rover": "Curiosity"},
        {"rover": "Perseverance"}
    ]
    assert result == expected_tasks
    assert len(result) == 3
    
    mock_logger.info.assert_any_call("3 Tasks Generated")

def test_generate_tasks_for_manifest_batch_special_rover_names(mock_logger):
    """Test task generation with special or unusual rover names"""
    rovers = ["Mars-2020", "test_rover", "ROVER_NAME", "rover with spaces"]
    
    result = generate_tasks_for_manifest_batch(rovers, mock_logger)
    
    expected_tasks = [
        {"rover": "Mars-2020"},
        {"rover": "test_rover"},
        {"rover": "ROVER_NAME"},
        {"rover": "rover with spaces"}
    ]
    assert result == expected_tasks
    assert len(result) == 4