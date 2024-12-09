from airbud import get_data, paginate_responses, upload_json_to_gcs, upload_to_bigquery

# Main function to ingest data (for use in an Airflow DAG)
def ingest_data(
    project_id,
    dataset_name,
    base_url,
    headers,
    bucket_name,
    endpoint,
    destination_blob_name,    
    paginate=False,    # Initialize pagination flag
    **kwargs 
):
    """
    Ingest data from a Recharge API endpoint into Google Cloud Storage and BigQuery.

   	**kwargs: Additional keyword arguments to pass to the request.
		jsonl_path (str): The path to the JSON data within the response.
		params (dict): The query parameters for the request.
		data (dict): The request body data.
		json_data (dict): The JSON data to send with the request.
		pagination_args (dict): The pagination arguments for the request.
    """
    # Set up variables for specific endpoint
    url = f"{base_url}{endpoint}"
    bucket_path = f"get/{endpoint}"
    jsonl_path = kwargs.get("jsonl_path", None)
    params = kwargs.get("params", None)
    data = kwargs.get("data", None)
    json_data = kwargs.get("json_data", None)
    
    # Get data
    if paginate:
        pagination_args = kwargs.get("pagination_args", None)
        records = paginate_responses(url, headers, jsonl_path, params, data, json_data, pagination_args)
    else:
        # Fetch data using get_data function
        response = get_data(url, headers, params, json_data, data)
        response_json = response.json()
        # Parse data (select specific path if jsonl_path is provided)
        records = response_json if jsonl_path is None else response_json.get(jsonl_path)
    
    # Upload raw data to GCS
    upload_json_to_gcs(records, bucket_name, bucket_path, destination_blob_name)
    
    # Land data in BigQuery
    upload_to_bigquery(project_id, endpoint, records)
