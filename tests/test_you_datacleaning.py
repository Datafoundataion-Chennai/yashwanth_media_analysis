import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest  # Added missing import
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.you_datacleaning import clean_and_reload_data

# Mock data for testing
MOCK_DATA = pd.DataFrame({
    "video_id": ["video1", "video2", "video3", "video1"],
    "publish_time": ["2023-01-01", "2023-01-02", None, "2023-01-01"],
    "views": [100, 200, None, 100],
    "title": ["Title 1", "Title 2", None, "Title 1"]
})

@patch('os.environ', {})
@patch("google.cloud.bigquery.Client")
def test_clean_and_reload_data_success(mock_client):
    """Test successful data cleaning and reload"""
    # Setup mock query result
    mock_query = MagicMock()
    mock_query.to_dataframe.return_value = MOCK_DATA.copy()
    mock_client.return_value.query.return_value = mock_query
    
    # Setup mock load job
    mock_job = MagicMock()
    mock_client.return_value.load_table_from_dataframe.return_value = mock_job
    
    # Call the function
    clean_and_reload_data()
    
    # Verify calls
    mock_client.return_value.query.assert_called_once()
    mock_client.return_value.load_table_from_dataframe.assert_called_once()

@patch('os.environ', {})
@patch("google.cloud.bigquery.Client")
def test_clean_and_reload_data_bad_request(mock_client, capsys):
    """Test handling of bad request error"""
    mock_client.return_value.query.side_effect = BadRequest("Invalid request")
    
    clean_and_reload_data()
    
    captured = capsys.readouterr()
    assert "Invalid request" in captured.out

@patch('os.environ', {})
@patch("google.cloud.bigquery.Client")
def test_clean_and_reload_data_general_error(mock_client, capsys):
    """Test handling of general exceptions"""
    mock_client.return_value.query.side_effect = Exception("General error")
    
    clean_and_reload_data()
    
    captured = capsys.readouterr()
    assert "Unexpected error" in captured.out