# Standard library imports
import logging
import json
import os

# Third-party imports
from airflow.models import TaskInstance
from airflow.models.dagrun import DagRun
from google.cloud import storage

# Local package imports
from custom_packages.airbud import get_data
from custom_packages.airbud import gcs

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def ingest_from_api(
    project_id: str,  # GCP Project
    bucket_name: str,  # GCS Bucket
    client: object,  # Client object
    endpoint: str,  # API endpoint
    endpoint_kwargs: dict,  # Endpoint-specific arguments
    paginate=False,  # Initialize pagination flag
    **kwargs
    ) -> str:

    # Initialize GCS client
    gcs_client = storage.Client(project_id)
    
    # API Endpoint parameters
    url = client.base_url + endpoint
    headers = client.headers or endpoint_kwargs.get("headers", None)
    jsonl_path = endpoint_kwargs.get("jsonl_path", None)
    params = endpoint_kwargs.get("params", None)
    data = endpoint_kwargs.get("data", None)
    json_data = endpoint_kwargs.get("json_data", None)

    log.info(f"Ingesting data from {endpoint} endpoint.")
    
    # Get data
    if paginate:
        log.info("Paginating data...")
        # Parse pagination parameters for use in client specific pagination method
        parameters = params or data or json_data or {}
        records, next_page = client.paginate_responses(endpoint, url, headers, parameters, **kwargs)

        # Store next page as XComs for downstream tasks
        task_instance = kwargs['task_instance']
        task_instance.xcom_push(
            key='next_page',
            value=next_page
        )
        log.info(f"Stored next page for {endpoint} in XComs: {next_page}")
    else:
        response = get_data.get_data(url, headers, params, json_data, data)
        response_json = response.json()
        # Parse data (select specific path)
        records = response_json if jsonl_path is None else response_json.get(jsonl_path)
    
    log.info(f"Completed data fetch for {endpoint}...")

    # Upload data to GCS
    if len(records) > 0:
        log.info(f"Uploading {len(records)} records to GCS...")
        gcs.upload_json_to_gcs(
            gcs_client,
            bucket_name,
            client.dataset,
            endpoint,
            records,
            **kwargs 
        )
        return "success"
    else:
        return (f"No records to upload for {endpoint}.")
