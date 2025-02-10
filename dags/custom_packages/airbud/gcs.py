# Standard imports
import os
import json
import logging
import pandas as pd
from typing import List, Dict
from datetime import datetime

# Third-party imports
from google.cloud import storage
from airflow.models.dagrun import DagRun


# Import Custom Libraries
from custom_packages.airbud import file_storage

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def generate_json_blob_name(
        dataset_name: str, 
        endpoint: str,
        supplemental: str = None,
        **kwargs
    ) -> (str, str):
    """Generate the destination blob name for a JSON file in GCS."""
    # Get DAG context
    dag_run: DagRun = kwargs.get('dag_run')
    dag_run_date = str(dag_run.logical_date)

    # Generate the GCS file path
    rooth_path = f"get/{dataset_name}/{endpoint}"
    if supplemental:
        filename = f"{rooth_path}/DAG_RUN:{dag_run_date}/{supplemental}.json"
    else:
        filename = f"{rooth_path}/DAG_RUN:{dag_run_date}.json"
    log.info(f"Generated GCS blob name: {filename}")
    return filename, dag_run_date

def upload_json_to_gcs(
    bucket: object, # GCS bucket client object
    filename: str,
    records: List[Dict]
    ) -> None:
    """Upload JSON data to Google Cloud Storage (GCS)."""

    log.info(f"Uploading json data to GCS...")
    # Upload the JSON data as a string to GCS
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(records), content_type='application/json')
    log.info(f"Uploaded json data to GCS: {filename}")

def get_records_from_file(
    bucket: object, # GCS bucket client object
    filename: str
    ) -> List[Dict]:
    """Get JSON data from Google Cloud Storage (GCS)."""
    try:
        blob = bucket.blob(filename)
    except Exception as e:
        log.warning("Error getting blob. Trying again...")
        blob = bucket.get_blob(filename)

    log.debug(f"Downloading data from {filename}...")
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
    ) -> List[str]:
    """List all files in a Google Cloud Storage (GCS) bucket."""
    
    # List all files in the GCS bucket under the given path
    blobs = bucket.list_blobs(prefix=path)
    files = sorted([blob.name for blob in blobs])

    return files

def upload_csv_to_gcs(
    bucket: object, # GCS bucket client object
    root_path: str,
    source_file: str,
    ) -> str:
    """Upload a CSV file to Google Cloud Storage (GCS)."""

    log.info(f"Uploading to csv to GCS...")
    # Get the filename
    filename = os.path.basename(source_file)
    if '.csv.csv' in filename:
        filename = filename.replace('.csv.csv', '.csv')

    # Set the GCS file path
    gcs_file_path = f"{root_path}/raw/{filename}"

    # Upload the file to GCS
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(source_file)
    log.info(f"Uploaded to csv to GCS: {blob.name}")

    filename_no_file_type = filename.replace('.csv', '')
    return filename_no_file_type

def load_files_to_gcs(
    gcs_bucket: object, # GCS bucket client object
    gcs_path: str, # GCS path
    dataset: str, # Dataset name
    endpoint: str, # Endpoint name
    source_file:str, # source file path (from the remote server)
    **kwargs
    ):
    # Upload raw csv file to GCS
    filename_no_file_type = upload_csv_to_gcs(gcs_bucket, gcs_path, source_file)

    # Generate the JSON blob name
    json_filename, dag_run_date = generate_json_blob_name(
        dataset, endpoint, supplemental=filename_no_file_type, **kwargs)
    
    # Clean the column names and convert to JSON
    records = file_storage.clean_column_names(source_file, json_filename, dag_run_date)
    
    # Upload the JSON data to GCS
    upload_json_to_gcs(gcs_bucket, json_filename, records)