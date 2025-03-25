import pytest
from unittest.mock import patch, Mock, MagicMock
import pandas as pd
from modules.newsdata_cleaning import clean_and_reload_data, clean_data

# Updated mock data with one exact duplicate (row 2 and row 3 are identical)
MOCK_DATA = pd.DataFrame({
    "category": ["News", None, "Sports", "Sports"],
    "headline": ["Headline 1", "Headline 2", None, None],
    "authors": ["Author 1", None, "Author 3", "Author 3"],
    "link": ["http://link1.com", "invalid", None, None],
    "short_description": ["Desc 1", None, "Desc 3", "Desc 3"],
    "date": ["2023-01-01", "2023-01-02", "invalid", "invalid"]
})

def test_clean_data():
    """Test the clean_data function independently"""
    cleaned_df = clean_data(MOCK_DATA)
    
    # Should have 3 rows after removing one duplicate (original had 4 with one exact duplicate)
    assert len(cleaned_df) == 3
    
    # Check NA values are handled
    assert cleaned_df["category"].isna().sum() == 0
    assert cleaned_df["headline"].isna().sum() == 0
    assert cleaned_df["authors"].isna().sum() == 0
    
    # Check URL validation
    assert cleaned_df["link"].str.contains("Invalid URL").sum() == 2
    
    # Check title case conversion
    assert all(cleaned_df["category"].str.istitle())
    assert all(cleaned_df["headline"].str.istitle())

@patch("google.cloud.bigquery.Client")
def test_clean_and_reload_data_success(mock_client):
    """Test successful execution of clean_and_reload_data"""
    # Setup mocks
    mock_query = MagicMock()
    mock_query.to_dataframe.return_value = MOCK_DATA
    mock_client.return_value.query.return_value = mock_query
    
    mock_job = MagicMock()
    mock_job.result.return_value = None
    mock_client.return_value.load_table_from_dataframe.return_value = mock_job
    
    # Call the function
    result = clean_and_reload_data()
    
    # Assertions
    assert result is True
    mock_client.return_value.query.assert_called_once()
    mock_client.return_value.load_table_from_dataframe.assert_called_once()

@patch("google.cloud.bigquery.Client")
def test_clean_and_reload_data_failure(mock_client):
    """Test failure scenario"""
    mock_client.return_value.query.side_effect = Exception("Test error")
    
    result = clean_and_reload_data()
    
    assert result is False