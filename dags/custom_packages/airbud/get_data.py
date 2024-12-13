import requests
import json
from typing import Dict, List
from airflow.models import Variable
import time

def get_data(
        url: str, # The URL of the API endpoint
        headers: dict, # The request headers
        params: dict, # The query parameters for the request, if any
        data: dict, # The request body data, if any
        json_data: dict # The JSON data to send with the request, if any
) -> Dict:
    """
    Get data from an API endpoint.

    Returns:
        dict: The JSON response from the API.
    """
    
    # Make the GET request with appropriate parameters
    response = requests.get(
        url,
        headers=headers,
        # Only pass if set, will be None if not
        params=params,  
        data=data,
        json=json_data
    )
    return response

def paginate_responses(
        url: str, # The URL of the API endpoint
        headers: str, # The request headers
        jsonl_path: str, # The path to the JSON data within the API response
        params: dict, # The query parameters for the request, if any
        data: dict, # The request body data, if any
        json_data: dict, # The JSON data to send with the request, if any
        pagination_args: dict# The pagination arguments for the request
) -> List[Dict]:
    """
    Paginate through the API endpoint.
    """
    # Unpack pagination arguments
    pagination_key = pagination_args.get("pagination_key")
    pagination_query = pagination_args.get("pagination_query")
    records = []

    # Initialize last page
    try:
        next_page = Variable.get(url)
        params, json_data, data = get_next_page_query(params, json_data, data, pagination_query, next_page)
    except:
        print("No next page stored. Starting pagination from the beginning.")
    while True:
        # Fetch data using get_data function
        response = get_data(url, headers, params, data, json_data)
        # Check for Rate Limiting
        if response.status_code == 429:
            for i in range(6):
                print(f"Rate Limit Exceeded. Waiting for 10 seconds before retrying.")
                time.sleep(10)
                response = get_data(url, headers, params, data, json_data)
                if response.status_code != 429:
                    break
        response_json = response.json()
        if jsonl_path:
            records.extend(response_json.get(jsonl_path))
        else:
            records.extend(response_json)
        # Check if there is another page of data
        next_page = response_json.get(pagination_key)
        if next_page:
            print(f"Fetching next page of data...{next_page}")
            params, json_data, data = get_next_page_query(params, json_data, data, pagination_query, next_page)
            last_page = next_page
        else:
            print("No more data to fetch.")
            break
    return records, last_page


# create next run variable if upload to bigquery is successful
def store_bookmark_for_next_page(url, next_page):
        Variable.set(url, next_page)
        print(f"Next page saved as Airflow Variable.")

def get_next_page_query(params, json_data, data, pagination_query, next_page):
    if params:
        params[pagination_query] = next_page
    elif json_data:
        json_data[pagination_query] = next_page
    elif data:
        data[pagination_query] = next_page
    return params, json_data, data
