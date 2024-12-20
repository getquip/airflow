from custom_packages import airbud
from typing import List, Dict
import pandas as pd
import json

# API Documentation: https://developer.rechargepayments.com/2021-11/cursor_pagination

def paginate_responses(
        endpoint: str, # The API endpoint
        url: str, # The URL of the API endpoint
        headers: str, # The request headers
        params: dict, # The query parameters for the request, if any
        **kwargs
    ) -> List[Dict]:
    
    # Set response limit to 250 (default is 50)
    params['limit'] = 250 
    
    # Get last bookmark
    if endpoint == "events":
    # For the `events` endpoint, we must paginate with no filters until we see the last ingested event_id
        last_event_id = airbud.get_next_page_from_last_dag_run("recharge__events")
    else:
    
    # For endpoints where data is updated in place, we must paginate using the updated_at field
         last_updated_at = airbud.get_next_page_from_last_dag_run(f"recharge__{endpoint}")
         if last_updated_at:
            params['updated_at'] = f">{last_updated_at}"
    
    # Paginate through the API endpoint and create a list of records
    records = []
    while True:
        response = airbud.get_data(url, headers, params, None, None)
            # Check for Rate Limiting
        if response.status_code != 200:
            response = retry_get_data(url, headers, params, None, None)
        # Parse response for records and append to records list
        response_json = response.json()
        records.extend(response_json.get(endpoint))
        
        if endpoint == "events":
            if last_event_id:
            # Check if last_event_id is in the last response
                df = pd.DataFrame(records)
                if last_event_id in df.id.values:
                    print(f"Found last event_id: {last_event_id}")
                    break
        
        # Check if there is another page of data
        next_page = response_json.get("next_cursor")
        if next_page:
            print(f"Fetching next page of data...{next_page}")
            params["cursor"] = next_page
        else:
            print("No more data to fetch.")
            break
        
    # Store bookmark for next run
    if endpoint == "events":
        max_event_created_at = df.created_at.max()
        last_event_id = df[df.created_at == max_event_created_at].id.max()
        print(last_event_id)
        #airbud.store_next_page_across_dags("recharge__events", last_event_id)
    else:
        df = pd.DataFrame(records)
        last_updated_at = df["updated_at"].max()
        print(last_updated_at)
        #airbud.store_next_page_across_dags(f"recharge__{endpoint}", last_updated_at)
    return records