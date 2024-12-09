import requests
import json

def get_data(url, headers, params, data, json_data):
    """
    Get data from an API endpoint.

    Args:
        url (str): The url of the API endpoint.
        headers (dict): The request headers.
        params (dict): The query parameters for the request.
        data (dict): The request body data.
        json_data (dict): The JSON data to send with the request.

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
    
    # Raise an error if the response status code is not 2xx
    response.raise_for_status()
    return response

def paginate_responses(url, headers, jsonl_path, params, data, json_data, pagination_args):
    """
    Paginate through the API endpoint.
    """
    # Unpack pagination arguments
    pagination_key = pagination_args.get("pagination_key")
    pagination_query = pagination_args.get("pagination_query")
    records = []
    while True:
        # Fetch data using get_data function
        response = get_data(url, headers, params, data, json_data)
        response_json = response.json()
        if jsonl_path:
            records.extend(response_json.get(jsonl_path))
        else:
            records.extend(response_json)
        # Check if there is another page of data
        next_page = response_json.get(pagination_key)
        if next_page:
            print(f"Fetching next page of data...{next_page}")
            if params:
                params[pagination_query] = next_page
            elif json_data:
                json_data[pagination_query] = next_page
            elif data:
                data[pagination_query] = next_page
        else:
            print("No more data to fetch.")
            break
    return records