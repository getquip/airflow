# Standard package imports
import pkgutil
from typing import List, Dict
import pandas as pd
import json
import logging

# Local package imports
from custom_packages.airbud import GetClient
from custom_packages.airbud.get_data import *

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

class GetWenParker(GetClient):
    def __init__(self):
        """
        Initializes the GetWenParker instance by setting up dataset, endpoints,
        schemas, and parent path.
        """
        super().__init__()  # Initialize the parent class

        # Define dataset name
        self.dataset = "wen_parker"

        # Define the parent path
        self.parent_path = "Quip/wen_parker"

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
	