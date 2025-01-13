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

class GetRecharge(GetClient):
    def __init__(self, auth: str):
        super().__init__() # Initialize the parent class
        self.dataset = "recharge"
        self.base_url = "https://api.rechargeapps.com/"
        self.headers = {
            "X-Recharge-Access-Token": auth,
            "X-Recharge-Version": "2021-11",
        }
        # Load endpoints configuration by passing the filename
        self.endpoints = self.load_endpoints(f"{ self.dataset }/endpoints.json")
        self.schemas = self.load_endpoint_schemas(self.dataset, self.endpoints.keys())
        

    def paginate_responses(
        self,
        endpoint: str,  # The API endpoint
        url: str,  # The URL of the API endpoint
        headers: Dict[str, str],  # The request headers
        parameters: Dict[str, str],  # The query parameters for the request, if any
        **kwargs,
    ) -> List[Dict]:
        # API Documentation: https://developer.rechargepayments.com/2021-11/cursor_pagination

        # Set response limit to 250 (default is 50)
        params = parameters
        params["limit"] = 250

        # Get last bookmark
        last_ts = get_next_page_from_last_dag_run(f"recharge__{endpoint}")
            # Only fetch the next day of data
        if last_ts:
            last_ts = pd.to_datetime(last_ts)
            stop_at = last_ts + pd.Timedelta(days=1)
            if endpoint == "events":
                params["created_at_min"] = last_ts
            else:
                params["updated_at_min"] = last_ts
            # If the last updated_at is today, do not pass the updated_at_max parameter
            if last_ts.date() == pd.Timestamp.utcnow().normalize().date() or stop_at.date() == pd.Timestamp.utcnow().normalize().date():
                pass
            # If the stop_at date is today, do not pass the updated_at_max parameter
            elif stop_at.date() == pd.Timestamp.utcnow().normalize().date():
                pass
            else:
                if endpoint == "events":
                    params["created_at_max"] = stop_at
                else:
                    params["updated_at_max"] = stop_at
                log.info(f"Fetching data from {last_ts} to {stop_at}")
        else:
            params["updated_at_max"] = '2024-06-20'

        # Paginate through the API endpoint and create a list of records
        records = []
        while True:
            response = get_data(url, headers, params, None, None)
            
            # Check for Rate Limiting or other errors
            if response.status_code != 200:
                response = retry_get_data(url, headers, params, None, None)
            if response.status_code == 200:
                # Parse response for records and append to records list
                response_json = response.json()
                records.extend(response_json.get(endpoint))
                
                # Check if there is another page of data
                next_page = response_json.get("next_cursor")
                if next_page:
                    # Only pass cursor as params
                    print(f"Fetching next page of data...{next_page}")
                    params = {"cursor": next_page}
                else:
                    print("No more data to fetch.")
                    break
            else:
                log.error(f"Pagination halted due to status code: {response.status_code}")
                break
            
        # Store bookmark for next run
        if len(records) > 0:
            df = pd.DataFrame(records)
            if endpoint == "events":
                next_page = df["created_at"].max()
            else:
                next_page = df["updated_at"].max()
        else:
            if last_ts:
                next_page = str(stop_at) 
            else:
                next_page = '2024-06-20'
        return records, next_page

    
