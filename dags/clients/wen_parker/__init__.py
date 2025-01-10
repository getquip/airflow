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
    def __init__(self, auth: str):
        super().__init__() # Initialize the parent class
        self.dataset = "wen_parker"
        # Load endpoints configuration by passing the filename
        self.endpoints = self.load_endpoints(f"{ self.dataset }/endpoints.json")
        self.schemas = self.load_endpoint_schemas(self.dataset, self.endpoints.keys())
		self.parent_path = "Quip/wen_parker/"
        
	