# Standard library imports
from logging import getLogger

# Third-party imports
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Local package imports
from custom_packages.airbud import get_data
from custom_packages.airbud import post_to_bigquery
from custom_packages.airbud import post_to_gcs


log = getLogger(__name__)


def download_client_files(
    bucket_name:str, # Host GCS bucket name
    prefix:str # Data source name
    ) -> None:
    """
    Downloads all files from a GCS directory to the local Airflow system.
    """
    # Initialize the GCS hook
    gcs_hook = GCSHook()

    # List all files under the specified prefix
    prefix = f"dags/clients/{prefix}"
    file_paths = gcs_hook.list(bucket_name=bucket_name, prefix=prefix)

    # Download each file from GCS to the local directory
    for file_path in file_paths:
        local_path = file_path.split("/")[-1]
        gcs_hook.download(bucket_name, file_path, f"/tmp/{local_path}")

def ingest_data(
    project_id: str,  # GCP project ID
    dataset_name: str,  # BigQuery dataset name
    gcs_bucket: str,  # GCS bucket name
    endpoint: str,  # API endpoint
    endpoint_kwargs: dict, # Endpoint-specific arguments
    paginate=False,    # Initialize pagination flag
    pagination_function=None,  # Initialize pagination arguments
    **kwargs
):
    """
    Ingest data from a Recharge API endpoint into Google Cloud Storage and BigQuery.
    """
    # Parse arguments
    ## Table Destination
    table_name = endpoint
    bigquery_metadata = endpoint_kwargs.get("bigquery_metadata")

    ## API Endpoint
    url = ingestion_metadata.get("base_url") + endpoint
    jsonl_path = endpoint_kwargs.get("jsonl_path", None)
    chunk_size = endpoint_kwargs.get("chunk_size", 8000)

    ## GCS Destination
    bucket_path = f"get/{dataset_name}/{endpoint}"
    destination_blob_name = endpoint_kwargs.get("destination_blob_name")

    log.info(f"Ingesting data from {dataset_name}'s {endpoint} endpoint.")
    # Get data
    if paginate:
        log.info("Paginating data...")
        # Parse pagination paramters
        parameters = params or data or json_data or {}
        records = pagination_function(endpoint, url, headers, parameters, **kwargs)
    else:
        response = get_data.get_data(url, headers, params, json_data, data)
        response_json = response.json()
        # Parse data (select specific path
        records = response_json if jsonl_path is None else response_json.get(jsonl_path)
    log.info(f"Completed data fetch...")
    
    # Upload raw data to GCS
    records = post_to_gcs.upload_json_to_gcs(project_id, records, gcs_bucket, bucket_path, destination_blob_name)
    log.info(f"Uploaded data to GCS location...{bucket_path}")
    
    # Land data in BigQuery
    post_to_bigquery.upload_to_bigquery(project_id, dataset_name, endpoint, bigquery_metadata, records, chunk_size)
    log.info(f"Completed data ingestion for {dataset_name}'s {endpoint} endpoint.")
