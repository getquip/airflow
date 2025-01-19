#INGEST DATA
import pandas as pd
from custom_packages import airbud
from clients.recharge import GetRecharge


project_id = 'quip-dw-raw'
API_KEY = airbud.get_secrets(project_id, 'recharge', prefix="api__")
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
import json

# Add metadata
df['source_synced_at'] = str(pd.Timestamp.utcnow())
records = json.loads(df.to_json(orient='records', lines=False))

# Get endpoint_kwargs
endpoint_kwargs = RECHARGE_CLIENT.endpoints[endpoint]

# Initialize BigQuery client
bq_client = RECHARGE_CLIENT.bq_client

# Ensure the destination table exists 
table_ref = bq_client.dataset("recharge").table(endpoint)

# Insert rows into BigQuery in chunks
chunk_size = endpoint_kwargs.get("chunk_size", 8000)
airbud.insert_records_to_bq(bq_client, table_ref, records, max_retries=3, chunk_size=chunk_size)

# Get the GCS bucket object
bucket = RECHARGE_CLIENT.gcs_bucket

# Upload the JSON data as a string to GCS
filename = f"get/recharge/{endpoint}/historical_to_2025.json"
blob = bucket.blob(filename)
blob.upload_from_string(json.dumps(records), content_type='application/json')
