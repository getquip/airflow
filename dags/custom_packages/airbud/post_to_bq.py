
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
        endpoint_kwargs: dict,
        endpoint: str,
        client_object: object
) -> None:
    """Create a BigQuery table if it does not exist."""
    try:
        # Try to fetch the table, if it exists, this will return the table
        table_ref = client.dataset(dataset_name).table(endpoint)
        client.get_table(table_ref)
        print(f"Table '{ endpoint }' already exists in dataset '{ dataset_name }'.")
    
    except Exception as e:
        print(f"Table '{endpoint}' does not exist'. Creating it now...")
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
            print(f"Table '{endpoint}' created successfully in dataset '{dataset_name}'.")   
        except Exception as e:
            print(f"An error occurred while trying to create the BigQuery table: {e}")
