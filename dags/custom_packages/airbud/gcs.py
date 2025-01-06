# Standard imports
import logging
import pandas as pd
import json
from typing import List, Dict
from datetime import datetime

# Third-party imports
from google.cloud import storage
from airflow.models.dagrun import DagRun

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def generate_blob_name(
        dataset_name: str, 
        endpoint: str,
        **kwargs
    ) -> str:
    """
    Generate the destination blob name for a JSON file in GCS.
    """
    dag_run: DagRun = kwargs.get('dag_run')
    file_path = f"get/{dataset_name}/{endpoint}"
    dag_run_date = dag_run.execution_date
    task_name = kwargs['task'].task_id
    filename = f"{file_path}/DAG_RUN:{dag_run_date}.json"

    return filename

def upload_json_to_gcs(
        project_id: str, 
        bucket_name: str, 
        dataset_name: str,
        endpoint: str,
        records: List[Dict],
        **kwargs
    ) -> None:
    """
    Upload JSON data to Google Cloud Storage (GCS).
    """
    # Initialize the GCS client
    client = storage.Client(project_id)

    # Get the GCS bucket object
    bucket = client.get_bucket(bucket_name)

    # Create the destination file name
    filename = generate_blob_name(dataset_name, endpoint, **kwargs)

    # Store synced_at timestamp in the records
    dag_run: DagRun = kwargs.get('dag_run')
    df = pd.DataFrame(records)
    df['source_synced_at'] = str(dag_run.execution_date)
    records = json.loads(df.to_json(orient='records', lines=False))

    # Upload the JSON data as a string to GCS
    blob = bucket.blob(filename)
    log.info(f"Uploading data to {filename}")
    blob.upload_from_string(json.dumps(records), content_type='application/json')
    log.info(f"Uploaded data to GCS location...{bucket_name}")

def get_records_from_file(
        project_id: str, 
        bucket_name: str, 
        dataset_name: str,
        endpoint: str,
        **kwargs
    ) -> List[Dict]:
    """
    Get JSON data from Google Cloud Storage (GCS).
    """
    # Generate the destination file name for the GCS blob
    filename = generate_blob_name(dataset_name, endpoint, **kwargs)

    # Initialize the GCS client
    client = storage.Client(project_id)

    # Get the GCS bucket object
    bucket = client.get_bucket(bucket_name)

    # Download the JSON data as a string from GCS
    blob = bucket.blob(filename)
    log.info(f"Downloading data from {filename}...")
    data = blob.download_as_string()
    try:
        json_data = data.decode("utf-8")
        records = json.loads(json_data)
    except Exception as e:
        log.error(f"Error parsing JSON data from GCS: {e}")
    log.info(f"Downloaded data from GCS location...{filename}")
    return records

