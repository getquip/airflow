# Standard package imports
import pkgutil
from typing import List, Dict
import pandas as pd
import json
import logging

# Local package imports
from custom_packages.airbud import GetClient
from custom_packages.airbud.api import *

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

class GetCeva(GetClient):
    def __init__(self):
        """
        Initializes the GetCeva class.

		Ceva data is sent to S3.
        """
        super().__init__()  # Initialize the parent class

        # Define dataset name
        self.dataset = "ceva"

        # Define the parent path
        self.bucket = "c5aa903e-2d4b-4853-b78c-af44763ec434"

        # Load endpoints configuration
        try:
            self.endpoints = self.load_endpoints(f"{self.dataset}/endpoints.json")
            log.info(f"Successfully loaded endpoints for dataset: {self.dataset}")
        except Exception as e:
            log.error(f"Failed to load endpoints configuration: {e}")
            self.endpoints = {}

        # Load endpoint schemas
        try:
            self.schemas = self.load_endpoint_schemas(self.dataset, self.endpoints.keys())
            log.info(f"Successfully loaded schemas for dataset: {self.dataset}")
        except Exception as e:
            log.error(f"Failed to load schemas for dataset {self.dataset}: {e}")
            self.schemas = {}

        log.info(f"Initialized GetWenParker with parent path: {self.parent_path}")
	