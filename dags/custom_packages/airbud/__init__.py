# Standard package imports
import pkgutil
import json
import os
import shutil
from typing import List, Dict

# Custom package imports
from custom_packages.airbud.gcs import *
from custom_packages.airbud.post_to_bq import *
from custom_packages.airbud.get_api import *
from custom_packages.airbud.secrets import *
from custom_packages.airbud.file_storage import *
from custom_packages.airbud.airflow import *


class GetClient:
    def __init__(self, project_id: str, bucket_name: str, dataset: str):
        self.project_id = project_id
        self.bucket_name = bucket_name

        # Initialize GCP clients
        self.bq_client = bigquery.Client(project_id)
        self.gcs_client = storage.Client(project_id)
        self.gcs_bucket = self.gcs_client.get_bucket(bucket_name)

        # Initialize BigQuery Dataset
        create_dataset_if_not_exists(self.bq_client, dataset)

    def load_endpoints(self, endpoint_file: str) -> Dict:
        """
        Loads the endpoints configuration from a specific JSON file.
        """

        try:
            package = self.__module__.split('.')[0]  # Get the package name of the class
            data = pkgutil.get_data(package, endpoint_file)  # Try to load the file from the correct package

            if data is not None:
                return json.loads(data.decode("utf-8"))
            else:
                raise FileNotFoundError(f"Could not find '{ endpoint_file }' in the package.")
        except Exception as e:
            raise RuntimeError(f"Failed to load endpoints: {e}")
            
    def load_endpoint_schemas(self, client_name, endpoints: List[str]) -> Dict:
        """
        Retrieves the schema for a given endpoint from the schemas directory.
        """
        schemas = {}
        for endpoint in endpoints:
            schema_file = f"{ client_name }/schemas/{ endpoint }.json"
            try:
                package = self.__module__.split('.')[0]  # Get the package name of the class
                data = pkgutil.get_data(package, schema_file)  # Try to load the file from the correct package

                if data is not None:
                    schemas[endpoint] = json.loads(data.decode("utf-8"))
                else:
                    raise FileNotFoundError(f"Could not find '{ endpoint }' schema in the package.")
            except Exception as e:
                raise RuntimeError(f"Failed to load endpoints: {e}")
        return schemas

    def generate_bq_tables(self) -> None:
        """
        Generates the BigQuery destination table name for a given endpoint.
        """
        # Parse client object
        bq_client = self.bq_client
        dataset = self.dataset

        for endpoint, endpoint_kwargs in self.endpoints.items():
            # Get the table reference
            table_ref = bq_client.dataset(dataset).table(endpoint)
            
            try: # Check if the table exists
                bq_client.get_table(table_ref)

            except Exception as e: # Create the dataset/table if it doesn't exist
                schema = self.schemas[endpoint]
                create_table_if_not_exists(bq_client, endpoint_kwargs, schema, table_ref)
                
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
