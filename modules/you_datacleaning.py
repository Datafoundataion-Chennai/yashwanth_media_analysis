import pandas as pd
import os
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, NotFound, BadRequest

def clean_and_reload_data():
    """Fetch data from BigQuery, clean it, and reload it."""
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\YASHWANTH\Desktop\Media Analytics\keys\media-content-analytics-454110-42ae23de3324.json"
        client = bigquery.Client()

        project_id = "media-content-analytics-454110"
        dataset_id = "News_Dataset"
        table_id = "youtubeapi_data"
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        query = f"SELECT * FROM {table_ref}"
        df = client.query(query).to_dataframe()

        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)
        df['publish_time'] = pd.to_datetime(df['publish_time'])

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Upload cleaned data to BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Successfully replaced old data with {df.shape[0]} cleaned rows in {table_ref}")
    except GoogleAPICallError as e:
        print(f"Google API Error: {e}")
    except NotFound as e:
        print(f"Table or dataset not found: {e}")
    except BadRequest as e:
        print(f"Invalid request: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    clean_and_reload_data()