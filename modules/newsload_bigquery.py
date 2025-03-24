from google.cloud import bigquery
import pandas as pd
import os
import json
from google.api_core.exceptions import GoogleAPICallError, NotFound, BadRequest

def load_data_to_bigquery():
    """Load data from a JSON file into BigQuery."""
    try:
        # Set Google Cloud credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\YASHWANTH\Desktop\Media Analytics\keys\media-content-analytics-454110-42ae23de3324.json"
        client = bigquery.Client()

        # Load JSON file
        json_file = r"C:\Users\YASHWANTH\Media content analytics\data\News_Category_Dataset.json"
        with open(json_file, "r", encoding="utf-8") as f:
            data = [json.loads(line) for line in f]

        # Convert JSON data to DataFrame
        df = pd.DataFrame(data)
        print("Sample Data:\n")
        print(df.head())

        # Define BigQuery job configuration
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("headline", "STRING"),
                bigquery.SchemaField("authors", "STRING"),
                bigquery.SchemaField("link", "STRING"),
                bigquery.SchemaField("short_description", "STRING"),
                bigquery.SchemaField("date", "STRING"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        # Upload data to BigQuery
        dataset_id = "News_Dataset"
        table_id = "News"
        table_ref = f"{client.project}.{dataset_id}.{table_id}"
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Successfully uploaded {job.output_rows} rows to BigQuery table {dataset_id}.{table_id}")

        # Query data from BigQuery
        query = f"SELECT * FROM {table_ref} LIMIT 10"
        query_job = client.query(query)
        result_df = query_job.to_dataframe()
        print("\nQueried Data from BigQuery:")
        print(result_df)
    except FileNotFoundError:
        print(f"Error: The file {json_file} was not found.")
    except json.JSONDecodeError:
        print(f"Error: The file {json_file} contains invalid JSON.")
    except NotFound as e:
        print(f"Error: The dataset or table {dataset_id}.{table_id} does not exist. Details: {e}")
    except BadRequest as e:
        print(f"Error: Invalid request to BigQuery. Details: {e}")
    except GoogleAPICallError as e:
        print(f"Error: Failed to upload data to BigQuery. Details: {e}")
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")

if __name__ == "__main__":
    load_data_to_bigquery()