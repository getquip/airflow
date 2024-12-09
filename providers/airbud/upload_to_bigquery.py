
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from airbud import upload_to_bigquery

def create_dataset_if_not_exists(client, PROJECT_ID, BQ_DATASET_NAME):
    """Create a BigQuery dataset if it does not exist."""
    dataset_ref = client.dataset(BQ_DATASET_NAME)
    
    try:
        # Try to fetch the dataset, if it exists, this will return the dataset
        client.get_dataset(dataset_ref)
        print(f"Dataset '{BQ_DATASET_NAME}' already exists.")
    except NotFound:
        # Dataset doesn't exist, so create it
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)  # API request to create the dataset
        print(f"Dataset '{BQ_DATASET_NAME}' created successfully.")

def create_table_if_not_exists(client, PROJECT_ID, BQ_DATASET_NAME, endpoint):
    """Create a BigQuery table if it does not exist."""
    table_ref = client.dataset(BQ_DATASET_NAME).table(endpoint)
    
    try:
        # Try to fetch the table, if it exists, this will return the table
        client.get_table(table_ref)
        print(f"Table '{endpoint}' already exists in dataset '{BQ_DATASET_NAME}'.")
    except NotFound: # Table doesn't exist, so create it
        # Get the destination schema from the JSON file
        with open(f"bigquery_destination_schemas/{BQ_DATASET_NAME}/{endpoint}.json", "r") as file:
            schema_data = json.load(file)

        # Convert the JSON schema into BigQuery SchemaField objects
        schema = [bigquery.SchemaField(field["name"], field["type"]) for field in schema_data]
        # Create table object
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)  # API request to create the table
        print(f"Table '{endpoint}' created successfully in dataset '{BQ_DATASET_NAME}'.")   
        
def upload_to_bigquery(PROJECT_ID, BQ_DATASET_NAME, endpoint, json_data):
    """
    Upload JSON data to BigQuery.

    Args:
        PROJECT_ID (str): Google Cloud Project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        json_data (list): List of JSON objects to insert into BigQuery.
    """
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    try:
        # Get the table reference
        table_ref = client.dataset(BQ_DATASET_NAME).table(endpoint)
    except Exception as e:
        # Create destination if it does not exist
        create_dataset_if_not_exists(client, PROJECT_ID, BQ_DATASET_NAME)
        create_table_if_not_exists(client, PROJECT_ID, BQ_DATASET_NAME, endpoint)
    
    # Insert rows into BigQuery
    try:
        # Insert the JSON data as rows into BigQuery
        errors = client.insert_rows_json(table_ref, json_data)
        
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
        else:
            print(f"Successfully inserted {len(json_data)} rows into {endpoint}.")
    
    except NotFound:
        print(f"Table {endpoint} does not exist in dataset {BQ_DATASET_NAME}.")
    except Exception as e:
        print(f"An error occurred: {e}")