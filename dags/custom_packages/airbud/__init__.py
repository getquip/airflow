# Standard package imports
import pkgutil
import json
import os
import shutil

# Import specific functions or classes you want to expose
from custom_packages.airbud.get_secrets import get_secrets
from custom_packages.airbud.get_data import *
from custom_packages.airbud.dag_operators.ingest_from_api import *
from custom_packages.airbud.dag_operators.load_data_to_bq import *
from custom_packages.airbud.dag_operators.ingest_from_sftp import *



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
