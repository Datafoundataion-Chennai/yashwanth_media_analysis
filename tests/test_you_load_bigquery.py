import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from google.cloud import bigquery
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.you_load_bigquery import (
    get_channel_statistics,
    get_channel_videos,
    get_video_details,
    load_data_to_bigquery
)

# Mock data for testing
MOCK_CHANNEL_STATS = {
    "items": [{
        "statistics": {
            "subscriberCount": "1000",
            "videoCount": "50"
        }
    }]
}

MOCK_VIDEO_IDS = {
    "items": [
        {"id": {"videoId": "video1"}},
        {"id": {"videoId": "video2"}}
    ],
    "nextPageToken": None
}

MOCK_VIDEO_DETAILS = {
    "items": [{
        "id": "video1",
        "snippet": {
            "title": "Video 1",
            "channelTitle": "Channel 1",
            "publishedAt": "2023-01-01T00:00:00Z",
            "description": "Description 1",
            "tags": ["tag1", "tag2"],
            "thumbnails": {"high": {"url": "http://thumbnail1.jpg"}},
            "categoryId": "1"
        },
        "statistics": {
            "viewCount": "100",
            "likeCount": "10",
            "commentCount": "5"
        }
    }]
}

# All passing tests (6 tests)
@patch("requests.get")
def test_get_channel_statistics(mock_get):
    """Test fetching channel statistics"""
    mock_get.return_value.json.return_value = MOCK_CHANNEL_STATS
    result = get_channel_statistics("channel_id")
    assert result == {"subscribers_count": 1000, "total_videos": 50}

@patch("requests.get")
def test_get_channel_statistics_empty(mock_get):
    """Test handling empty channel stats"""
    mock_get.return_value.json.return_value = {"items": []}
    result = get_channel_statistics("channel_id")
    assert result == {"subscribers_count": 0, "total_videos": 0}

@patch("requests.get")
def test_get_channel_videos(mock_get):
    """Test fetching video IDs from channel"""
    mock_get.return_value.json.return_value = MOCK_VIDEO_IDS
    result = get_channel_videos("channel_id")
    assert result == ["video1", "video2"]

@patch("requests.get")
def test_get_video_details(mock_get):
    """Test fetching video details"""
    mock_get.return_value.json.return_value = MOCK_VIDEO_DETAILS
    result = get_video_details(["video1"])
    assert len(result) == 1
    assert result[0]["video_id"] == "video1"

@patch('os.environ', {})
@patch("google.cloud.bigquery.Client")
@patch("modules.you_load_bigquery.get_all_channel_data")
def test_load_data_to_bigquery_success(mock_get_data, mock_client):
    """Test successful BigQuery load"""
    mock_df = pd.DataFrame({
        "video_id": ["vid1"],
        "publish_time": ["2023-01-01"],
        "views": [100],
        "channel_name": ["Channel 1"]
    })
    mock_df['publish_time'] = pd.to_datetime(mock_df['publish_time'])
    mock_get_data.return_value = mock_df
    mock_job = MagicMock()
    mock_client.return_value.load_table_from_dataframe.return_value = mock_job
    load_data_to_bigquery()
    mock_client.return_value.load_table_from_dataframe.assert_called_once()

@patch('os.environ', {})
@patch("google.cloud.bigquery.Client")
@patch("modules.you_load_bigquery.get_all_channel_data")
def test_load_data_to_bigquery_empty(mock_get_data, mock_client):
    """Test handling empty DataFrame"""
    mock_get_data.return_value = pd.DataFrame()
    load_data_to_bigquery()
    mock_client.return_value.load_table_from_dataframe.assert_not_called()