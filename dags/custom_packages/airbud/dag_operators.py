
from dags.custom_packages.airbud.get_data import *
from dags.custom_packages.airbud.post_to_bigquery import *
from dags.custom_packages.airbud.post_to_gcs import *
from dags.custom_packages.airbud.get_secrets import *
from logging import getLogger

log = getLogger(__name__)

def ingest_data(
    project_id: str,
    dataset_name: str,
    base_url: str,
    headers: dict,
    bucket_name: str,
    endpoint: str,
    destination_blob_name: dict,
    bigquery_metadata: dict,
    paginate=False,    # Initialize pagination flag
    **kwargs
):
    """
    Ingest data from a Recharge API endpoint into Google Cloud Storage and BigQuery.

   	**kwargs:
		jsonl_path (str): The path to the JSON data within the response.
		params (dict): The query parameters for the request.
		data (dict): The request body data.
		json_data (dict): The JSON data to send with the request.
		pagination_args (dict): The pagination arguments for the request.
    """
    log.info(f"Ingesting data from {dataset_name}'s {endpoint} endpoint.")
    # Set up variables for specific endpoint
    url = f"{base_url}{endpoint}"
    bucket_path = f"get/{dataset_name}/{endpoint}"
    jsonl_path = kwargs.get("jsonl_path", None)
    params = kwargs.get("params", None)
    data = kwargs.get("data", None)
    json_data = kwargs.get("json_data", None)

    # Get data
    if paginate:
        pagination_args = kwargs.get("pagination_args", None)
        log.info("Paginating data...")
        records = paginate_responses(url, headers, jsonl_path, params, data, json_data, pagination_args)
    else:
        # Fetch data using get_data function
        response = get_data(url, headers, params, json_data, data)
        response_json = response.json()
        # Parse data (select specific path if jsonl_path is provided)
        records = response_json if jsonl_path is None else response_json.get(jsonl_path)
    log.info(f"Completed data fetch...")
    # Upload raw data to GCS
    upload_json_to_gcs(project_id, records, bucket_name, bucket_path, destination_blob_name)
    log.info(f"Uploaded data to GCS...")
    # Land data in BigQuery
    upload_to_bigquery(project_id, dataset_name, endpoint, bigquery_metadata, records)
    log.info(f"Completed data ingestion for {dataset_name}'s {endpoint} endpoint.")
