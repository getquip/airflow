from google.cloud import storage
from google.api_core.exceptions import NotFound
import pandas as pd
import json

def upload_json_to_gcs(json_data, GCS_BUCKET_NAME, BUCKET_PATH, destination_blob_name):
    """
    Upload JSON data to Google Cloud Storage (GCS).

    Args:
        json_data (dict): The JSON data to upload.
        GCS_BUCKET_NAME (str): The name of the GCS bucket.
        BUCKET_PATH (str): The path within the bucket to store the data.
        destination_blob_name (str): The name of the destination blob.
    """
    # Convert each object in the list of json to a json string
    def yield_jsonl():
        for line in json_data:
            yield json.dumps(line)
    
    # Initialize the GCS client
    client = storage.Client(PROJECT_ID)
    # Get the GCS bucket object
    bucket = client.get_bucket("airflow_outputs")
    
    # Create the destination file name
    df = pd.DataFrame(json_data)
    start_date = df[destination_blob_name['date_range']].min()
    end_date = df[destination_blob_name['date_range']].max()
    filename = f"{BUCKET_PATH}/DAG_RUN:{destination_blob_name['dag_run_date']}_{destination_blob_name['date_range']}:{start_date}_to_{end_date}.json"
    
    # Upload the JSON data as a string to GCS
    blob = bucket.blob(filename)
    blob.upload_from_string(",\n".join(yield_jsonl()), content_type='application/json')
