from google.cloud import storage
import pandas as pd
import json
from typing import List, Dict

def upload_json_to_gcs(project_id: str, json_data: List[Dict], bucket_name: str, bucket_path: str, destination_blob_name: dict) -> None:
    """
    Upload JSON data to Google Cloud Storage (GCS).

    Args:
        project_id (str): The ID of the GCP project.
        json_data (List[Dict]): The JSON data to upload.
        bucket_name (str): The name of the GCS bucket.
        bucket_path (str): The path within the bucket to store the data.
        destination_blob_name (str): The name of the destination blob.
    """
    # Convert each object in the list of json to a json string
    def yield_jsonl():
        for line in json_data:
            yield json.dumps(line)
    
    # Initialize the GCS client
    client = storage.Client(project_id)
    # Get the GCS bucket object
    bucket = client.get_bucket("airflow_outputs")
    
    # Create the destination file name
    df = pd.DataFrame(json_data)
    start_date = df[destination_blob_name['date_range']].min()
    end_date = df[destination_blob_name['date_range']].max()
    filename = f"{bucket_path}/DAG_RUN:{destination_blob_name['dag_run_date']}_{destination_blob_name['date_range']}:{start_date}_to_{end_date}.json"
    
    # Upload the JSON data as a string to GCS
    blob = bucket.blob(filename)
    blob.upload_from_string(",\n".join(yield_jsonl()), content_type='application/json')