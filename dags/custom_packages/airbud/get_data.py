import requests
import json
from typing import Dict, List
import time
import logging

from airflow.models import Variable

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def get_data(
        url: str, # The URL of the API endpoint
        headers: dict, # The request headers
        params: dict, # The query parameters for the request, if any
        data: dict, # The request body data, if any
        json_data: dict # The JSON data to send with the request, if any
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
        params: dict, # The query parameters for the request, if any
        data: dict, # The request body data, if any
        json_data: dict # The JSON data to send with the request, if any
    ) -> object: # The response object
    """Retries the API request."""
    max_retries = 6
    for attempt in range(max_retries):
        response = get_data(url, headers, params, data, json_data)
        if response.status_code == 200:
            break
        elif attempt == max_retries - 1:
            logging.warn(f"API unresponsive after {max_retries} retries.", exc_info=True)
            break
        else:
            if response.status_code == 429:
                logging.info(f"Rate Limit Exceeded. Waiting for 10 seconds before retrying.")
            else:
                logging.warn(f"Response: {response.status_code}. Retrying in 10 seconds...")
        time.sleep(10)
    return response


# create next run variable if upload to bigquery is successful
def store_next_page_across_dags(name: str, last_page: str) -> None:
    """Store the last page in an Airflow Variable."""
    Variable.set(name, last_page)
    logging.info(f"Last page saved as Airflow Variable named: {name}.")

def get_next_page_from_last_dag_run(name:str) -> str:
    """Retrieve the last page from an Airflow Variable."""
    try:
        next_page = Variable.get(name)
        logging.info(f"Retrieved next page: {next_page}")
    except:
        logging.info("No next page stored. Starting pagination from the beginning.")
        next_page = None
    return(next_page)

def get_next_page_query(
    params: dict,
    json_data: dict,
    data: dict,
    pagination_query: str,
    next_page: str
    ) -> tuple:
    """Add the next page to the query parameters."""
    if params:
        params[pagination_query] = next_page
    elif json_data:
        json_data[pagination_query] = next_page
    elif data:
        data[pagination_query] = next_page
    return params, json_data, data