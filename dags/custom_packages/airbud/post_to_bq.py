
from google.cloud import bigquery
from typing import List, Dict
import json
import time
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def create_dataset_if_not_exists(
        client: object, # BigQuery client object
        dataset_name: str
) -> None:
    """Create a BigQuery dataset if it does not exist."""
    dataset_ref = client.dataset(dataset_name)
    
    try:
        # Try to fetch the dataset, if it exists, this will return the dataset
        client.get_dataset(dataset_ref)
        log.info(f"Dataset '{dataset_name}' already exists.")

    except Exception as e:
        log.info(e)
        # Dataset doesn't exist, so create it
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)  # API request to create the dataset
        log.info(f"Dataset '{dataset_name}' created successfully.")

def create_table_if_not_exists(
        client: object, # BigQuery client object
        dataset_name: str,
        endpoint_kwargs: dict,
        endpoint: str,
        client_object: object, # DAG client object
        table_ref: object # BigQuery table reference
) -> None:
    """Create a BigQuery table if it does not exist."""
    try:
        # Try to fetch the table, if it exists, this will return the table
        client.get_table(table_ref)
        log.info(f"Table '{ endpoint }' already exists in dataset '{ dataset_name }'.")
    
    except Exception as e:
        log.info(f"Table '{endpoint}' does not exist'. Creating it now...")
        bigquery_metadata = endpoint_kwargs.get("bigquery_metadata", {})
        # Get the destination schema from the JSON file
        schema = client_object.schemas[endpoint]
        
        # Create table object
        table = bigquery.Table(table_ref, schema=schema)
        
        # Set partitioning
        if bigquery_metadata["partitioning_type"] == 'DAY':
            partition_by = bigquery.TimePartitioningType.DAY
        elif bigquery_metadata["partitioning_type"] == 'MONTH':
            partition_by = bigquery.TimePartitioningType.MONTH
        elif bigquery_metadata["partitioning_type"] == 'YEAR':
            partition_by = bigquery.TimePartitioningType.YEAR
        table.time_partitioning = bigquery.TimePartitioning(
            type_=partition_by,
            field=bigquery_metadata["partitioning_field"]
        )
        
        # Set clustering
        table.clustering_fields = bigquery_metadata.get("clustering_fields", None)
        
        # Create table in BQ
        try:
            client.create_table(table)
            log.info(f"Table '{endpoint}' created successfully in dataset '{dataset_name}'.")   
        except Exception as e:
            log.error(f"An error occurred while trying to create the BigQuery table: {e}")

def insert_records(
        client: object,  # BigQuery client object
        table_ref: object,  # BigQuery table reference
        records: List[Dict],  # List of records to insert
        max_retries: int = 3,  # Maximum number of retries
        chunk_size: int = 1000,  # Number of records per chunk
    ) -> None:
    total_records = len(records)
    log.info(f"Starting data insertion for {total_records} records in chunks of {chunk_size}.")

    for i in range(0, total_records, chunk_size):
        chunk = records[i:i + chunk_size]
        for attempt in range(max_retries):
            try:
                errors = client.insert_rows_json(table_ref, chunk)
                if errors:
                    log.error(f"Errors encountered in chunk {i}-{i + len(chunk)}: {errors}")
                    raise RuntimeError(f"Insertion failed for chunk {i}-{i + len(chunk)}")
                else:
                    log.info(f"Successfully inserted chunk {i}-{i + len(chunk)}.")
                    break  # Exit retry loop on success
            except Exception as e:
                log.warning(f"Attempt {attempt + 1} failed for chunk {i}-{i + len(chunk)}: {e}")
                if attempt == max_retries - 1:
                    log.error(f"Max retries reached for chunk {i}-{i + len(chunk)}. Failing.")
                    raise RuntimeError(f"Unable to insert data after {max_retries} retries for chunk {i}-{i + len(chunk)}.")
                time.sleep(retry_delay)

    log.info(f"All {total_records} records inserted successfully into the table.")

def get_destination(
        bq_client: object, # BigQuery client object
        client: object, # DAG client object
        endpoint: str, 
        endpoint_kwargs: dict 
    ) -> object:
    table_ref = bq_client.dataset(client.dataset).table(endpoint)
    try:
        bq_client.get_table(table_ref)
    except Exception as e:
        # Dataset and Table creation logic
        create_dataset_if_not_exists(bq_client, client.dataset)
        create_table_if_not_exists(bq_client, client.dataset, endpoint_kwargs, endpoint, client)
        
        # Retry logic for table availability
        max_retries = 5
        for attempt in range(max_retries):
            try:
                bq_client.get_table(table_ref)
                log.info(f"Table { endpoint } is now available.")
                break
            except Exception:
                if attempt == max_retries - 1:
                    raise RuntimeError(f"Table { endpoint } not found after { max_retries } retries.")
                log.warning(f"Table { endpoint } not found. Retrying in 5 seconds...")
                time.sleep(5)
    return table_ref