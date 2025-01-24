# Standard imports
import time
import json
import logging
import requests
from typing import Dict, List

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def get_data(
        url: str, # The URL of the API endpoint
        headers: dict, # The request headers
        params: dict =None, # The query parameters for the request, if any
        data: dict =None, # The request body data, if any
        json_data: dict =None # The JSON data to send with the request, if any
    ) -> Dict: # The response object
    """Get data from an API endpoint."""
    response = requests.get(
        url,
        headers=headers,
        # Only pass if set, will be None if not
        params=params,  
        data=data,
        json=json_data
    )
    return response

def retry_get_data(
        url: str, # The URL of the API endpoint
        headers: dict, # The request headers
        params: dict =None, # The query parameters for the request, if any
        data: dict =None, # The request body data, if any
        json_data: dict =None # The JSON data to send with the request, if any
    ) -> object: # The response object
    """Retries the API request."""
    max_retries = 6
    for attempt in range(max_retries):
        response = get_data(url, headers, params, data, json_data)
        if response.status_code == 200:
            break
        elif attempt == max_retries - 1:
            log.warning(f"API unresponsive after {max_retries} retries.", exc_info=True)
            break
        else:
            if response.status_code == 429:
                log.info(f"Rate Limit Exceeded. Waiting for 10 seconds before retrying.")
            else:
                log.warning(f"Response: {response.status_code}. Retrying in 10 seconds...")
        time.sleep(10)
    return response
