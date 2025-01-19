# Standard imports
import json
import logging
import pandas as pd
from typing import List, Dict
from datetime import datetime

# Third-party imports
from google.cloud import storage
from airflow.models.dagrun import DagRun

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def generate_blob_name(
        dataset_name: str, 
        endpoint: str,
        **kwargs
    ) -> str:
    """Generate the destination blob name for a JSON file in GCS."""
    dag_run: DagRun = kwargs.get('dag_run')
    file_path = f"get/{dataset_name}/{endpoint}"
    dag_run_date = dag_run.execution_date
    filename = f"{file_path}/DAG_RUN:{dag_run_date}.json"

    return filename

def upload_json_to_gcs(
    client: object, # GCS client object
    bucket_name: str, 
    dataset: str,
    endpoint: str,
    records: List[Dict],
    **kwargs
    ) -> None:
    """Upload JSON data to Google Cloud Storage (GCS)."""
    # Get the GCS bucket object
    bucket = client.get_bucket(bucket_name)

    # Generate the GCS file path
    filename = generate_blob_name(dataset, endpoint, **kwargs)

    # Store synced_at timestamp in the records
    dag_run: DagRun = kwargs.get('dag_run')
    df = pd.DataFrame(records)
    df['source_synced_at'] = str(dag_run.execution_date)
    records = json.loads(df.to_json(orient='records', lines=False))

    # Upload the JSON data as a string to GCS
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(records), content_type='application/json')
    log.debug(f"Uploaded data to GCS: {filename}")

def get_records_from_file(
        client: object, # GCS client object
        bucket_name: str, 
        dataset: str,
        endpoint: str,
        **kwargs
    ) -> List[Dict]:
    """Get JSON data from Google Cloud Storage (GCS)."""
    # Get the GCS bucket object
    bucket = client.get_bucket(bucket_name)

    # Generate the GCS file path
    filename = generate_blob_name(dataset, endpoint, **kwargs)

    # Download the JSON data as a string from GCS
    blob = bucket.blob(filename)
    log.debug(f"Downloading data from {filename}...")
    data = blob.download_as_string()
    try:
        json_data = data.decode("utf-8")
        records = json.loads(json_data)
    except Exception as e:
        raise Exception(f"Error parsing JSON data from GCS: {e}")
    return records

def list_all_files(
    client: object, # GCS client object
    bucket_name: str, 
    path: str,
    **kwargs
    ) -> List[str]:
    """List all files in a Google Cloud Storage (GCS) bucket."""
    # Get the GCS bucket object
    bucket = client.get_bucket(bucket_name)

    # List all files in the GCS bucket under the given path
    blobs = bucket.list_blobs(prefix=path, **kwargs)
    files = sorted([blob.name for blob in blobs])

    return files

def upload_csv_to_gcs(
    client: object, # GCS client object
    bucket_name: str,
    root_path: str,
    filename: str,
    **kwargs
    ) -> None:
    """Upload a CSV file to Google Cloud Storage (GCS)."""
    # Initialize the GCS path
    bucket = client.get_bucket(bucket_name)

    # Set the GCS file path
    gcs_file_path = f"{root_path}/raw/{filename}"

    # Upload the file to GCS
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(filename)
    log.info(f"Uploaded to GCS: gs://{bucket_name}/{gcs_file_path}")
