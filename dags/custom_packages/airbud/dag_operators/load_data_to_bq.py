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

def load_data_to_bq_from_api(
        project_id: str,  # Destination Project ID
        bucket_name: str,  # Source bucket
        client: object,  # client object
        endpoint: str,  # Destination table
        endpoint_kwargs: dict,  # Metadata for BigQuery table creation
        paginate=False,  # Initialize pagination flag
        **kwargs  # Additional keyword arguments
) -> None:
    """
    Upload JSON data to BigQuery.
    """
    # Get records from file or API
    try:
        # Initialize GCS client
        gcs_client = storage.Client(project_id)
        records = gcs.get_records_from_file(
            gcs_client,
            bucket_name,
            client.dataset,
            endpoint,
            **kwargs
        )
    except Exception as e:
        log.error(f"Failed to get records from file: { e }")
        records = []
    
    if len(records) > 0:
        # Initialize BigQuery client
        bq_client = bigquery.Client(project=project_id)
        
        # Ensure the destination table exists
        table_ref = post_to_bq.get_destination(bq_client, client, endpoint, endpoint_kwargs)

        # Insert rows into BigQuery in chunks
        chunk_size = endpoint_kwargs.get("chunk_size", 8000)
        post_to_bq.insert_records(bq_client, table_ref, records, max_retries=3, chunk_size=chunk_size)
        log.info(f"Successfully inserted { len(records) } rows into { endpoint }.")
    else:
        log.info(f"No records found for { endpoint }. Skipping load.")
    # Store bookmark for endpoint (if applicable)
    if paginate:
        # Get the next_page from XComs (from the upstream task)
        task_instance = kwargs['ti']  # This gives you the TaskInstance
        upstream_task = f'get__{ endpoint }.ingest_{ endpoint }_data'
        next_page = task_instance.xcom_pull(task_ids=upstream_task, key='next_page')

        if next_page:
            bookmark_name = f"{ client.dataset }__{ endpoint }"
            store_next_page_across_dags(bookmark_name, next_page)
            log.info(f"Stored next page for { endpoint }.")
        else:
            log.info(f"No next page found for { endpoint }.")

