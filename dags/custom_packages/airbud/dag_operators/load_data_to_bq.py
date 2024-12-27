# Standard library imports
import json
import time
import logging

# Third-party imports
from google.cloud import bigquery
from airflow.models import TaskInstance

# Local package imports
from custom_packages.airbud import post_to_bq
from custom_packages.airbud import get_data, store_next_page_across_dags
from custom_packages.airbud import gcs

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def load_data_to_bq(
        project_id: str,  # Destination Project ID
        bucket_name: str,  # Source bucket
        dataset_name: str,  # Destination dataset
        endpoint: str,  # Destination table
        endpoint_kwargs: dict,  # Metadata for BigQuery table creation
        paginate=False,  # Initialize pagination flag
        **kwargs  # Additional keyword arguments
) -> None:
    """
    Upload JSON data to BigQuery.
    """
    # Get records from file or API
    records = gcs.get_records_from_file(
        project_id,
        bucket_name,
        dataset_name,
        endpoint,
        **kwargs
    )
    
    if not records:
        log.warning(f"No records found for {endpoint}. Skipping load.")
        return

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Ensure the destination table exists
    table_ref = client.dataset(dataset_name).table(endpoint)
    try:
        client.get_table(table_ref)
    except Exception as e:
        # Dataset and Table creation logic
        post_to_bq.create_dataset_if_not_exists(client, project_id, dataset_name)
        post_to_bq.create_table_if_not_exists(client, project_id, dataset_name, endpoint_kwargs, endpoint)
        
        # Retry logic for table availability
        max_retries = 5
        for attempt in range(max_retries):
            try:
                client.get_table(table_ref)
                log.info(f"Table {endpoint} is now available.")
                break
            except Exception:
                if attempt == max_retries - 1:
                    raise RuntimeError(f"Table {endpoint} not found after {max_retries} retries.")
                log.warning(f"Table {endpoint} not found. Retrying in 5 seconds...")
                time.sleep(5)

    # Insert rows into BigQuery in chunks
    max_retries = 3
    chunk_size = endpoint_kwargs.get("chunk_size", 8000)
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        for attempt in range(max_retries):
            try:
                errors = client.insert_rows_json(table_ref, chunk)
                if errors:
                    log.error(f"Encountered errors while inserting rows: {errors}")
                    raise RuntimeError(f"Insertion failed for chunk {i}-{i+chunk_size}")
                else:
                    log.info(f"Successfully inserted chunk {i}-{i+chunk_size}.")
                    break
            except Exception as e:
                log.warning(f"Retrying due to error: {e}")
                if attempt == max_retries - 1:
                    raise RuntimeError(f"Unable to insert data after {max_retries} retries.")
                time.sleep(5)

    log.info(f"Successfully inserted {len(records)} rows into {endpoint}.")

    # Store bookmark for endpoint (if applicable)
    if paginate:
        # Get the next_page from XComs (from the upstream task)
        task_instance = kwargs['ti']  # This gives you the TaskInstance
        upstream_task = f'get__{endpoint}.ingesting_{endpoint}_data'
        next_page = task_instance.xcom_pull(task_ids=upstream_task, key='next_page')

        if next_page:
            bookmark_name = f"{dataset_name}__{endpoint}"
            store_next_page_across_dags(bookmark_name, next_page)
            log.info(f"Stored next page for {endpoint}.")
        else:
            log.info(f"No next page found for {endpoint}.")
