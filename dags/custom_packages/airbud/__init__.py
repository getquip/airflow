# Standard package imports
import pkgutil
import json
import os
import shutil

# Import specific functions or classes you want to expose
from custom_packages.airbud.get_secrets import get_secrets
from custom_packages.airbud.get_data import *
from custom_packages.airbud.dag_operators.ingest_data import *
from custom_packages.airbud.dag_operators.load_data_to_bq import *



class GetClient:
    def __init__(self):
        self.base_url = None
        self.headers = None

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
                raise FileNotFoundError(f"Could not find '{endpoint_file}' in the package.")
        except Exception as e:
            raise RuntimeError(f"Failed to load endpoints: {e}")
            
    def get_endpoint_schema(self, endpoint: str) -> Dict:
        """
        Retrieves the schema for a given endpoint from the schemas directory.
        """
        schema_file = f"schemas/{endpoint}.json"
        
        try:
            # Load the schema JSON file from the package
            schema_data = pkgutil.get_data(__package__, schema_file)
            if schema_data is None:
                raise FileNotFoundError(f"Schema file '{schema_file}' not found in the package.")
            
            # Parse the schema data
            schema = json.loads(schema_data.decode("utf-8"))
            return schema
        
        except Exception as e:
            raise RuntimeError(f"Failed to load schema for '{endpoint}': {e}")

def cleanup_tmp_files(dag_id: str):
    """
    Cleanup temporary files in the .tmp directory.
    """
    # Delete the directory and all its contents
    directory = f".tmp/{dag_id}"
    if os.path.exists(directory):
        shutil.rmtree(directory)
        log.info(f"Deleted directory: {directory}")
    else:
        log.info(f"Directory {directory} does not exist.")