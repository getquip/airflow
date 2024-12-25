from google.cloud import storage
import pandas as pd
import json
from typing import List, Dict
from datetime import timezone, datetime


def upload_json_to_gcs(
        project_id: str, 
        json_data: List[Dict], 
        bucket_name: str, 
        bucket_path: str, 
        destination_blob_name: dict
    ) -> None:
    """
    Upload JSON data to Google Cloud Storage (GCS).
    """
    # Convert each object in the list of json to a json string
    def yield_jsonl():
        for line in json_data:
            yield json.dumps(line)
    
    # Initialize the GCS client
    client = storage.Client(project_id)
    # Get the GCS bucket object
    bucket = client.get_bucket(bucket_name)
    
    # Create the destination file name
    df = pd.DataFrame(json_data)
    start_date = df[destination_blob_name['date_range']].min()
    end_date = df[destination_blob_name['date_range']].max()
    filename = f"{bucket_path}/DAG_RUN:{destination_blob_name['dag_run_date']}_{destination_blob_name['date_range']}:{start_date}_to_{end_date}.json"
    
    # Upload the JSON data as a string to GCS
    blob = bucket.blob(filename)
    print(f"Uploading data to {filename}...")
    blob.upload_from_string(",\n".join(yield_jsonl()), content_type='application/json')

    # Store synced_at timestamp
    dt = datetime.now(timezone.utc) 
    utc_time = dt.replace(tzinfo=timezone.utc)
    df['source_synced_at'] = str(utc_time)
    return json.loads(df.to_json(orient='records', lines=False))

def get_file_from_gcs(
        project_id: str, 
        bucket_name: str, 
        object_name: str, 
        file_type: str
    ) -> None:
    """
    Download a file from Google Cloud Storage (GCS).
    """
    # Initialize the GCS client
    client = storage.Client(project_id)
    # Get the GCS bucket object
    bucket = client.get_bucket(bucket_name)
    
    # Get the blob object
    blob = bucket.blob(object_name)

    if file_type == 'json':
        # Download the blob content as text (assuming the file is JSON)
        file_content = blob.download_as_text()
        file = json.loads(file_content)
        print(f"Downloaded and parsed JSON file from GCS: {object_name}.")
    else:
        # Download the blob content as a byte string
        file_content = blob.download_as_string()
        file = file_content.decode('utf-8')
        print(f"Downloaded file from GCS: {object_name}.")
    return file