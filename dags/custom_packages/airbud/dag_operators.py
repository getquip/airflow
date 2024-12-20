
from custom_packages.airbud import get_data
from custom_packages.airbud import post_to_bigquery
from custom_packages.airbud import post_to_gcs
from logging import getLogger

log = getLogger(__name__)

def ingest_data(
    ingestion_metadata: dict, # Metadata for ingestion
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
    project_id = ingestion_metadata.get("project_id", "quip-de-raw-dev")
    dataset_name = ingestion_metadata.get("dataset_name", "airflow")
    table_name = endpoint
    bigquery_metadata = endpoint_kwargs.get("bigquery_metadata")

    ## API Endpoint
    url = ingestion_metadata.get("base_url") + endpoint
    jsonl_path = endpoint_kwargs.get("jsonl_path", None)
    params = endpoint_kwargs.get("params", None)
    data = endpoint_kwargs.get("data", None)
    json_data = endpoint_kwargs.get("json_data", None)
    headers = endpoint_kwargs.get("headers", ingestion_metadata.get("headers"))

    ## GCS Destination
    bucket_name = ingestion_metadata.get("gcs_bucket_name", "airflow_outputs")
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
    records = post_to_gcs.upload_json_to_gcs(project_id, records, bucket_name, bucket_path, destination_blob_name)
    log.info(f"Uploaded data to GCS location...{bucket_path}")
    
    # Land data in BigQuery
    post_to_bigquery.upload_to_bigquery(project_id, dataset_name, endpoint, bigquery_metadata, records)
    log.info(f"Completed data ingestion for {dataset_name}'s {endpoint} endpoint.")