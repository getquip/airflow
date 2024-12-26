# Standard library imports
from logging import getLogger

# Local package imports
from custom_packages.airbud import get_data
from custom_packages.airbud import post_to_bigquery
from custom_packages.airbud import post_to_gcs

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def ingest_data(
    project_id: str,  # GCP project ID
    gcs_bucket: str,  # GCS bucket name
    client: object,  # Client object
    endpoint: str,  # API endpoint
    endpoint_kwargs: dict, # Endpoint-specific arguments
    paginate=False,    # Initialize pagination flag
    **kwargs
):
    """
    Ingest data from a Recharge API endpoint into Google Cloud Storage and BigQuery.
    """
    # Parse arguments
    ## Table Destination
    dataset_name = client.dataset
    table_name = endpoint
    bigquery_metadata = endpoint_kwargs.get("bigquery_metadata")

    ## API Endpoint
    url = client.base_url + endpoint
    headers = client.headers or endpoint_kwargs.get("headers", None)
    jsonl_path = endpoint_kwargs.get("jsonl_path", None)
    params = endpoint_kwargs.get("params", None)
    data = endpoint_kwargs.get("data", None)
    json_data = endpoint_kwargs.get("json_data", None)
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
        records = client.paginate_responses(endpoint, url, headers, parameters, **kwargs)
    else:
        response = get_data.get_data(url, headers, params, json_data, data)
        response_json = response.json()
        # Parse data (select specific path
        records = response_json if jsonl_path is None else response_json.get(jsonl_path)
    log.info(f"Completed data fetch...")
    
    # Upload raw data to GCS
    records = post_to_gcs.upload_json_to_gcs(
        project_id, 
        records, 
        gcs_bucket, 
        bucket_path, 
        destination_blob_name
    )
    log.info(f"Uploaded data to GCS location...{bucket_path}")
    
    # Land data in BigQuery
    post_to_bigquery.upload_to_bigquery(
        project_id, 
        dataset_name, 
        endpoint, 
        bigquery_metadata, 
        records, 
        chunk_size
    )
    log.info(f"Completed data ingestion for {dataset_name}'s {endpoint} endpoint.")
