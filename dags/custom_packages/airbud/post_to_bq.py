
# Import Standard Libraries
import os
import json
import time
import logging
from typing import List, Dict

# Import Third Party Libraries
from google.cloud import bigquery

# Custom Libraries
from custom_packages.airbud.gcs import get_records_from_file, generate_json_blob_name

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
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
        log.info(f"BigQuery Dataset '{dataset_name}' created successfully.")

def create_table_if_not_exists(
        client: object, # BigQuery client object
        endpoint_kwargs: dict,
        schema: dict, # Schema for the table
        table_ref: object # BigQuery table reference
    ) -> None:
    """Create a BigQuery table if it does not exist."""
    try:
        # Try to fetch the table, if it exists, this will return the table
        client.get_table(table_ref)
        log.info(f"Table '{table_ref}' already exists.")
    
    except Exception as e:
        log.info(f"Table '{table_ref}' does not exist'. Creating it now...")
        bigquery_metadata = endpoint_kwargs.get("bigquery_metadata", {})
        
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
            log.info(f"{table_ref} created successfully.")   
        except Exception as e:
            raise Exception(f"{e} An error occurred while trying to create {table_ref}: {e}")

def insert_records_to_bq(
        client: object,  # BigQuery client object
        table_ref: object,  # BigQuery table reference
        records: List[Dict],  # List of records to insert
        max_retries: int = 3,  # Maximum number of retries
        chunk_size: int = 1000,  # Number of records per chunk
    ) -> None:
    """Insert records into a BigQuery table in chunks."""
    total_records = len(records)
    log.info(f"""
        Starting data insertion for {total_records} records in chunks of
        {total_records if total_records < chunk_size else chunk_size}.
        """)

    for i in range(0, total_records, chunk_size):
        chunk = records[i:i + chunk_size]
        for attempt in range(max_retries):
            try:
                errors = client.insert_rows_json(table_ref, chunk)
                if errors:
                    log.error(f"Errors encountered in chunk {i}-{i + len(chunk)}: {errors}")
                    raise RuntimeError(f"Insertion failed for chunk {i}-{i + len(chunk)}")
                else:
                    log.info(f"Successfully inserted chunk {i}-{i + len(chunk)}")
                    break  # Exit retry loop on success
            except Exception as e:
                log.warning(f"Attempt {attempt + 1} failed for chunk {i}-{i + len(chunk)}: {e}")
                if attempt == max_retries - 1:
                    log.error(f"Max retries reached for chunk {i}-{i + len(chunk)}. Failing.")
                    raise RuntimeError(
                        f"""Unable to insert data after {max_retries} retries for chunk {i}-{i + len(chunk)}.""")
                time.sleep(5)

    log.info(f"All {total_records} records inserted successfully into the table.")

def insert_files_to_bq(
    files: List[str], 
    endpoint: str, 
    dataset: str,
    bq_client: object,
    gcs_bucket: str,
    **kwargs
    ) -> (List[str], List[str]):
    files_to_move = []
    bad_files = []
    # Process each file
    for source_file in files:
        # Get file name
        local_file = os.path.basename(source_file)
        log.info(f"Processing {source_file}...")
        
        # 1. Get records from GCS
        try:
            supplemental = local_file.split(".")[0]
            filename, dag_run_date = generate_json_blob_name(
                dataset, 
                endpoint, 
                supplemental=supplemental, 
                **kwargs
            )
            records  = get_records_from_file(gcs_bucket, filename)
            log.info(f"Successfully loaded {len(records)} records from GCS.")
        except Exception as e:
            log.error(f"Failed to get records from file: {e}")
            bad_files.append(local_file)
            records = []

        # 2. Insert the records to BigQuery
        if len(records) > 0:
            try:
                table_ref = bq_client.dataset(dataset).table(endpoint)
                
                insert_records_to_bq(bq_client, table_ref, records)
                log.info(f"Successfully uploaded {local_file} to BigQuery.")
                files_to_move.append(source_file)
            except Exception as e:
                log.error(f"Error uploading {local_file} to BigQuery: {e}")
                bad_files.append(local_file)
        
    # 3. Check if there are any bad files
    if len(bad_files) > 0:
        log.error(f"Failed to process { len(bad_files) } files: {bad_files}")
    if len(files_to_move) > 0:
        log.info(f"Successfully uploaded { len(files_to_move) } files to BigQuery.")
    if len(files_to_move) == 0 and len(files) > 0:
        raise Exception(f"None of the {len(files)} files downloaded were successfully processed.")
    return files_to_move, bad_files
