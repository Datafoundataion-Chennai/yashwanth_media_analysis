from google.cloud import bigquery
import pandas as pd
import os
import re
from google.api_core.exceptions import GoogleAPICallError, NotFound, BadRequest

def clean_and_reload_data():
    """Fetch data from BigQuery, clean it, and reload it."""
    try:
        # Set Google Cloud credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\YASHWANTH\Desktop\Media Analytics\keys\media-content-analytics-454110-42ae23de3324.json"
        client = bigquery.Client()

        # Define dataset and table references
        dataset_id = "News_Dataset"
        table_id = "News"
        table_ref = f"{client.project}.{dataset_id}.{table_id}"

        # Query data from BigQuery
        query = f"SELECT * FROM {table_ref}"
        query_job = client.query(query)
        df = query_job.to_dataframe()
        print("Raw Data Sample:\n", df.head())

        # Data Cleaning
        df = df.drop_duplicates()
        df["category"] = df["category"].fillna("Unknown")
        df["headline"] = df["headline"].fillna("No Headline")
        df["authors"] = df["authors"].fillna("Anonymous")
        df["short_description"] = df["short_description"].fillna("")
        df["link"] = df["link"].fillna("No Link")

        # Standardize text formatting
        df["headline"] = df["headline"].str.strip().str.title()
        df["category"] = df["category"].str.strip().str.title()
        df["authors"] = df["authors"].str.strip().str.title()
        df["short_description"] = df["short_description"].str.strip()

        # Convert date column to date format
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

        # Validate URLs
        def is_valid_url(url):
            return bool(re.match(r"https?://[^\s]+", url))

        df["link"] = df["link"].apply(lambda x: x if is_valid_url(x) else "Invalid URL")

        print("Cleaned Data Sample:\n", df.head())

        # Upload cleaned data back to BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Successfully uploaded cleaned data to {table_ref}")
    except NotFound as e:
        print(f"Error: The dataset or table {dataset_id}.{table_id} does not exist. Details: {e}")
    except BadRequest as e:
        print(f"Error: Invalid request to BigQuery. Details: {e}")
    except GoogleAPICallError as e:
        print(f"Error: Failed to upload data to BigQuery. Details: {e}")
    except Exception as e:
        print(f"Error uploading cleaned data to BigQuery: {e}")

if __name__ == "__main__":
    clean_and_reload_data()