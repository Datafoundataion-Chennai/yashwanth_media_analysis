import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import json
import os
import sys
from google.api_core.exceptions import GoogleAPICallError

# Add parent directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the module to test
from modules.newsload_bigquery import load_data_to_bigquery

# Mock data for testing
MOCK_JSON_DATA = [
    {"category": "POLITICS", "headline": "Sample headline", "authors": "John Doe", 
     "link": "http://example.com", "short_description": "Sample description", "date": "2023-01-01"},
    {"category": "TECH", "headline": "Tech news", "authors": "Jane Smith", 
     "link": "http://tech.com", "short_description": "Tech description", "date": "2023-01-02"}
]

@patch('os.environ', {})
@patch("google.cloud.bigquery.Client")
@patch("builtins.open")
def test_successful_load(mock_open, mock_client):
    """Test successful loading of data to BigQuery"""
    # Mock the JSON file reading
    mock_file = MagicMock()
    mock_file.__enter__.return_value = mock_file
    mock_file.__iter__.return_value = [json.dumps(item) for item in MOCK_JSON_DATA]
    mock_open.return_value = mock_file

    # Mock BigQuery client and methods
    mock_job = MagicMock()
    mock_job.result.return_value = None
    mock_job.output_rows = len(MOCK_JSON_DATA)
    mock_client.return_value.load_table_from_dataframe.return_value = mock_job

    # Mock query results
    mock_query_job = MagicMock()
    mock_query_job.to_dataframe.return_value = pd.DataFrame(MOCK_JSON_DATA)
    mock_client.return_value.query.return_value = mock_query_job

    # Call the function
    load_data_to_bigquery()

    # Verify the file was opened
    mock_open.assert_called_once()
    # Verify data was loaded to BigQuery
    mock_client.return_value.load_table_from_dataframe.assert_called_once()
    # Verify data was queried back
    mock_client.return_value.query.assert_called_once()

@patch('os.environ', {})
@patch("google.cloud.bigquery.Client")
@patch("builtins.open")
def test_invalid_json(mock_open, mock_client, capsys):
    """Test handling of invalid JSON"""
    # Mock file to return invalid JSON
    mock_file = MagicMock()
    mock_file.__enter__.return_value = mock_file
    mock_file.__iter__.return_value = ["invalid json line"]
    mock_open.return_value = mock_file

    load_data_to_bigquery()
    
    captured = capsys.readouterr()
    assert "Error: The file" in captured.out
    assert "contains invalid JSON" in captured.out

@patch('os.environ', {})
@patch("google.cloud.bigquery.Client")
@patch("builtins.open")
def test_general_exception(mock_open, mock_client, capsys):
    """Test handling of general exceptions"""
    # Mock file reading to raise general exception
    mock_file = MagicMock()
    mock_file.__enter__.return_value = mock_file
    mock_file.__iter__.side_effect = Exception("General error")
    mock_open.return_value = mock_file

    load_data_to_bigquery()
    
    captured = capsys.readouterr()
    assert "Error uploading data to BigQuery" in captured.out