
from google.cloud import bigquery
from typing import List, Dict
import json
import time

def create_dataset_if_not_exists(
        client,
        project_id: str,
        dataset_name: str
) -> None:
    """Create a BigQuery dataset if it does not exist."""
    dataset_ref = client.dataset(dataset_name)
    
    try:
        # Try to fetch the dataset, if it exists, this will return the dataset
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_name}' already exists.")

    except Exception as e:
        print(e)
        # Dataset doesn't exist, so create it
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)  # API request to create the dataset
        print(f"Dataset '{dataset_name}' created successfully.")

def create_table_if_not_exists(
        client,
        project_id: str,
        dataset_name: str,
        bigquery_metadata: dict,
        endpoint: str
) -> None:
    """Create a BigQuery table if it does not exist."""
    try:
        # Try to fetch the table, if it exists, this will return the table
        table_ref = client.dataset(dataset_name).table(endpoint)
        client.get_table(table_ref)
        print(f"Table '{endpoint}' already exists in dataset '{dataset_name}'.")
    
    except Exception as e:
        print(f"Table '{endpoint}' does not exist'. Creating it now...")
        # Get the destination schema from the JSON file
        schema = client.schema_from_json(f"clients/{dataset_name}/schemas/{endpoint}.json")
        
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
            print(f"Table '{endpoint}' created successfully in dataset '{dataset_name}'.")   
        except Exception as e:
            print(f"An error occurred while trying to create the BigQuery table: {e}")
        
def upload_to_bigquery(
        project_id: str,  # Destination Project ID
        dataset_name: str,  # Destination dataset
        endpoint: str,  # Destination table
        bigquery_metadata: dict,  # Metadata for BigQuery table creation
        records: List[Dict],  # List of JSON objects to insert into BigQuery
        chunk_size: int,  # Number of rows to insert at a time
) -> None:
    """
    Upload JSON data to BigQuery.
    """
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Ensure the dataset exists
    create_dataset_if_not_exists(client, project_id, dataset_name)
    
    # Ensure the table exists
    create_table_if_not_exists(client, project_id, dataset_name, bigquery_metadata, endpoint)
    table_ref = client.dataset(dataset_name).table(endpoint)
    
    # Check if the table is available
    max_retries=5
    for attempt in range(max_retries):
        try:
            client.get_table(table_ref)
            print(f"Table {endpoint} is now available.")
            break
        except Exception as e:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Table {endpoint} not found after {max_retries} retries.")
            print(f"Table {endpoint} not found. Retrying in 5 seconds...")
            time.sleep(5)

    # Insert rows into BigQuery
    max_retries=3
    # Insert data in chunks -- insert limit is 10,000 rows
    for i in range(0, len(records), chunk_size):
        for attempt in range(max_retries):
            chunk = records[i:i + chunk_size]
            errors = client.insert_rows_json(table_ref, chunk)
            if attempt == max_retries - 1:
                raise RuntimeError(f"Unable to insert data after {max_retries} retries.")
            elif errors:
                print(f"Encountered errors while inserting rows: {errors}")
                print(f"Retrying...")
            else: # exit attempt loop if no errors
                print("Successfully inserted chunk.")
                break
    print(f"Successfully inserted {len(records)} rows into {endpoint}.")