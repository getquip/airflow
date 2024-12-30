# SET airflow/ TO WORKING DIRECTORY
import json
from google.cloud import bigquery

# Import endpoint_kwargs from the client
CLIENT = "recharge"
with open(f"dags/clients/{CLIENT}/endpoints.json", "r") as f:
    endpoint_kwargs = json.load(f)

# Initialize the BigQuery client
client = bigquery.Client()

# Iterate over endpoints and their kwargs
for endpoint, kwargs in endpoint_kwargs.items():
    # Define source and destination table
    source_table = f"quip-dw-raw-dev.{CLIENT}.{endpoint}"
    destination_table = f"quip-dw-raw.{CLIENT}.{endpoint}"
    
    # Get Partitioning and Clustering metadata
    partitioning_field = kwargs['bigquery_metadata'].get('partitioning_field', '_PARTITIONDATE') # 
    partitioning_type = kwargs['bigquery_metadata'].get('partitioning_type', 'DAY')
    clustering_fields = kwargs['bigquery_metadata'].get('clustering_fields', None)
    
    # Define a query to deduplicate the source table
    query = f"""
    SELECT * 
    FROM `{source_table}`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY {partitioning_field} DESC) = 1
    """
    
    # Configure the job for writing results to a new table with partitioning and clustering
    job_config = bigquery.QueryJobConfig(
        destination=destination_table,  # Target table
        write_disposition="WRITE_TRUNCATE",  # Overwrite the table if it exists
    )
    
    # Set up time partitioning
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY if partitioning_type.upper() == "DAY" else bigquery.TimePartitioningType.HOUR,
        field=partitioning_field,  # Partition on this column
        require_partition_filter=True,  # Enforce partition filtering
    )
    
    # Add clustering if specified
    if clustering_fields:
        job_config.clustering_fields = clustering_fields
    
    # Execute the query
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        print(f"Table {destination_table} created with partitioning and clustering.")
    except Exception as e:
        print(f"Failed to create table {destination_table}: {e}")
