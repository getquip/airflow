#INGEST DATA
import pandas as pd
from custom_packages.airbud.get_data import get_data, retry_get_data
from custom_packages.airbud.get_secrets import get_secrets
from clients.recharge import GetRecharge


project_id = 'quip-dw-raw'
API_KEY = get_secrets(project_id, 'recharge', prefix="api__")
RECHARGE_CLIENT = GetRecharge(API_KEY['api_key'])

endpoint = 'charges'
url = f"{RECHARGE_CLIENT.base_url}/{endpoint}"
headers = RECHARGE_CLIENT.headers
params = {
	"limit": 250,
	"updated_at_min": "2024-06-20T00:00:00+00:00",
	"updated_at_max": "2024-06-21T00:00:00+00:00"
}
records = []

stop_backfill = pd.to_datetime('2025-01-01T00:00:00+00:00')

# Paginate through the API endpoint and create a list of backfill records
while pd.to_datetime(params["updated_at_max"]) < stop_backfill:
    u_min= params["updated_at_min"]
    u_max= params["updated_at_max"]
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
        
    # prep next batch
    df = pd.DataFrame(records)
    params = {}
    params["updated_at_min"] = max(u_max, df["updated_at"].max())
    params["updated_at_max"] = str(pd.to_datetime(params["updated_at_min"]) + pd.Timedelta(days=1))
    params['limit'] = 250
    print(f"Fetching data from {params['updated_at_min']} to {params['updated_at_max']}")
    print(len(records))


## UPLOAD TO BIGQUERY
from google.cloud import bigquery
from custom_packages.airbud import post_to_bq

endpoint_kwargs = RECHARGE_CLIENT.endpoints[endpoint]

bq_client = bigquery.Client(project=project_id)
# Ensure the destination table exists 
table_ref = post_to_bq.get_destination(bq_client, client, endpoint, endpoint_kwargs)
