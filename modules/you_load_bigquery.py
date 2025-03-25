import os
import requests
import pandas as pd
import time
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, NotFound, BadRequest

API_KEY = "add your own api key"

CHANNELS = {
    "TV9 Telugu": "UCfaww9Q8C_-EaM0sXI8o-fA",
    "NTV Telugu": "UCumtYpCY26F6Jr3satUgMvA",
    "TV5 News": "UCAR3h_9fLV82N2FH4cE4RKw",
    "ABN Andhra Jyothi": "UC_2irx_BQR7RsBKmUV9fePQ",
    "Sakshi TV": "UCZ9m4KOh8Ei60428xeGYDCQ",
    "V6 News": "UCDCMjD1XIAsCZsYHNMGVcog",
    "T News": "UCu6edg8_eu3-A8ylgaWereA",
    "10TV News Telugu": "UCfymZbh17_3T_UhgjkQ9fRQ",
    "99TV": "UCl5YgCiwSRVOiC2Nd1P9v1A",
    "ETV Andhra Pradesh": "UCJi8M0hRKjz8SLPvJKEVTOg"
}

def get_channel_statistics(channel_id):
    """Fetch channel-level statistics like total videos, subscribers."""
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "statistics",
        "id": channel_id,
        "key": API_KEY
    }
    try:
        response = requests.get(url, params=params).json()
        if "items" in response and len(response["items"]) > 0:
            stats = response["items"][0]["statistics"]
            return {
                "subscribers_count": int(stats.get("subscriberCount", 0)),  # Convert to int
                "total_videos": int(stats.get("videoCount", 0)),  # Convert to int
            }
        return {"subscribers_count": 0, "total_videos": 0}
    except requests.exceptions.RequestException as e:
        print(f"Error fetching channel statistics for {channel_id}: {e}")
        return {"subscribers_count": 0, "total_videos": 0}

def get_channel_videos(channel_id):
    """Fetch video IDs from a channel."""
    base_url = "https://www.googleapis.com/youtube/v3/search"
    video_ids = []
    next_page_token = None

    while len(video_ids) < 10000:
        params = {
            "part": "id",
            "channelId": channel_id,
            "maxResults": 50,
            "order": "date",
            "type": "video",
            "pageToken": next_page_token,
            "key": API_KEY
        }
        try:
            response = requests.get(base_url, params=params).json()
            if "items" in response:
                video_ids.extend([item["id"]["videoId"] for item in response["items"]])
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching videos for {channel_id}: {e}")
            break

    return video_ids[:10000]

def get_video_details(video_ids):
    """Fetch detailed video statistics and metadata."""
    video_url = "https://www.googleapis.com/youtube/v3/videos"
    videos = []

    for i in range(0, len(video_ids), 50):
        params = {
            "part": "snippet,statistics",
            "id": ",".join(video_ids[i:i+50]),
            "key": API_KEY
        }
        try:
            response = requests.get(video_url, params=params).json()
            for item in response.get("items", []):
                snippet = item["snippet"]
                stats = item.get("statistics", {})
                videos.append({
                    "video_id": item["id"],
                    "title": snippet["title"],
                    "channel_title": snippet["channelTitle"],
                    "category_id": snippet.get("categoryId", ""),
                    "publish_time": snippet["publishedAt"],
                    "description": snippet.get("description", ""),
                    "tags": ", ".join(snippet.get("tags", [])),
                    "thumbnail_link": snippet["thumbnails"]["high"]["url"],
                    "video_link": f"https://www.youtube.com/watch?v={item['id']}",
                    "views": int(stats.get("viewCount", 0)),  # Convert to int
                    "likes": int(stats.get("likeCount", 0)),  # Convert to int
                    "comment_count": int(stats.get("commentCount", 0)),  # Convert to int
                    "comments_disabled": "commentCount" not in stats
                })
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching video details: {e}")
            continue

    return videos

def get_all_channel_data():
    """Combine all data for multiple channels."""
    all_videos = []

    for channel_name, channel_id in CHANNELS.items():
        print(f"Fetching data for {channel_name}...")
        channel_stats = get_channel_statistics(channel_id)
        video_ids = get_channel_videos(channel_id)
        if not video_ids:
            print(f"[WARNING] No videos found for {channel_name}")
            continue
        videos = get_video_details(video_ids)
        for video in videos:
            video.update({
                "channel_name": channel_name,
                "subscribers_count": channel_stats["subscribers_count"],
                "total_videos": channel_stats["total_videos"]
            })
        all_videos.extend(videos)

    return pd.DataFrame(all_videos)

def load_data_to_bigquery():
    """Load data into BigQuery."""
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\YASHWANTH\Desktop\Media Analytics\keys\media-content-analytics-454110-42ae23de3324.json"
        client = bigquery.Client()

        project_id = "media-content-analytics-454110"
        dataset_id = "News_Dataset"
        table_id = "youtubeapi_data"
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        df = get_all_channel_data()

        print("DataFrame Columns:", df.columns)

        if df.empty:
            print("[WARNING] No data fetched. Skipping BigQuery upload.")
            return

        # Convert publish_time to datetime64 for BigQuery compatibility
        df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')

        schema = [
            bigquery.SchemaField("video_id", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("channel_title", "STRING"),
            bigquery.SchemaField("category_id", "STRING"),
            bigquery.SchemaField("publish_time", "TIMESTAMP"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("tags", "STRING"),
            bigquery.SchemaField("thumbnail_link", "STRING"),
            bigquery.SchemaField("video_link", "STRING"),
            bigquery.SchemaField("views", "INTEGER"),
            bigquery.SchemaField("likes", "INTEGER"),
            bigquery.SchemaField("comment_count", "INTEGER"),
            bigquery.SchemaField("comments_disabled", "BOOLEAN"),
            bigquery.SchemaField("channel_name", "STRING"),
            bigquery.SchemaField("subscribers_count", "INTEGER"),
            bigquery.SchemaField("total_videos", "INTEGER"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"[SUCCESS] Successfully replaced old data with {df.shape[0]} new rows in {table_ref}")
    except GoogleAPICallError as e:
        print(f"[ERROR] Google API Error: {e}")
    except NotFound as e:
        print(f"[ERROR] Table or dataset not found: {e}")
    except BadRequest as e:
        print(f"[ERROR] Invalid request: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")


if __name__ == "__main__":
    load_data_to_bigquery()
