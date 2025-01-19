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

def generate_json_blob_name(
        dataset_name: str, 
        endpoint: str,
        supplemental: str = None,
        **kwargs
    ) -> str:
    """Generate the destination blob name for a JSON file in GCS."""
    # Get DAG context
    dag_run: DagRun = kwargs.get('dag_run')
    dag_run_date = dag_run.execution_date

    # Generate the GCS file path
    rooth_path = f"get/{dataset_name}/{endpoint}"
    if supplemental:
        filename = f"{rooth_path}/DAG_RUN:{dag_run_date}/{supplemental}.json"
    else:
        filename = f"{rooth_path}/DAG_RUN:{dag_run_date}.json"

    return filename

def upload_json_to_gcs(
    bucket: object, # GCS bucket client object
    filename: str,
    records: List[Dict],
    **kwargs
    ) -> None:
    """Upload JSON data to Google Cloud Storage (GCS)."""

    # Store synced_at timestamp in the records
    df = pd.DataFrame(records)
    dag_run: DagRun = kwargs.get('dag_run')
    df['source_synced_at'] = str(dag_run.execution_date)
    records = json.loads(df.to_json(orient='records', lines=False))

    # Upload the JSON data as a string to GCS
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(records), content_type='application/json')
    log.info(f"Uploaded data to GCS: {filename}")

def get_records_from_file(
    bucket: object, # GCS bucket client object
    filename: str
    ) -> List[Dict]:
    """Get JSON data from Google Cloud Storage (GCS)."""

    # Download the JSON data as a string from GCS
    blob = bucket.blob(filename)
    log.info(f"Downloading data from {filename}...")
    data = blob.download_as_string()

    # Parse the JSON data
    try:
        json_data = data.decode("utf-8")
        records = json.loads(json_data)
    except Exception as e:
        raise Exception(f"Error parsing JSON data from GCS: {e}")
        
    return records

def list_all_files(
    bucket: object, # GCS bucket client object
    path: str,
    **kwargs
    ) -> List[str]:
    """List all files in a Google Cloud Storage (GCS) bucket."""

    # List all files in the GCS bucket under the given path
    blobs = bucket.list_blobs(prefix=path, **kwargs)
    files = sorted([blob.name for blob in blobs])

    return files

def upload_csv_to_gcs(
    bucket: object, # GCS bucket client object
    root_path: str,
    filename: str,
    **kwargs
    ) -> None:
    """Upload a CSV file to Google Cloud Storage (GCS)."""
    
    # Set the GCS file path
    gcs_file_path = f"{root_path}/raw/{filename}"

    # Upload the file to GCS
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(filename)
    log.info(f"Uploaded to GCS: {blob.name}")
