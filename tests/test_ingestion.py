from unittest.mock import patch, MagicMock
from src.ingestion import extract_photos_from_nasa

def test_extract_photos_from_nasa():
    """Test the NASA API extraction function"""
    logger = MagicMock()
    
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "photos": [
                {"id": 1, "img_src": "http://example.com/photo1.jpg"},
                {"id": 2, "img_src": "http://example.com/photo2.jpg"}
            ]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = extract_photos_from_nasa("Perseverance", 1, logger)
        
        assert result == {
            "photos": [
                {"id": 1, "img_src": "http://example.com/photo1.jpg"},
                {"id": 2, "img_src": "http://example.com/photo2.jpg"}
            ]
        }
        
        mock_get.assert_called_once()
        assert "rovers/Perseverance/photos" in mock_get.call_args[0][0]
        assert "sol=1" in mock_get.call_args[0][0]

def test_extract_photos_from_nasa_error():
    """Test error handling in NASA API extraction"""
    logger = MagicMock()
    
    with patch('requests.get') as mock_get:
        mock_get.side_effect = Exception("API Error")
        
        result = extract_photos_from_nasa("Perseverance", 1, logger)
        assert result == {"photos": []}
        logger.error.assert_called()
