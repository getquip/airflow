#INGEST DATA
import pandas as pd
from custom_packages import airbud
from clients.recharge import GetRecharge


project_id = 'quip-dw-raw'
bucket_name = 'quip_airflow'
RECHARGE_CLIENT = GetRecharge(project_id, bucket_name)

endpoint = 'events'
url = f"{RECHARGE_CLIENT.base_url}{endpoint}"
headers = RECHARGE_CLIENT.headers
params = {
	"limit": 250,
	"cursor": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTAyLTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiA5NzQwNzk5MDIwLCAibGFzdF92YWx1ZSI6IDk3NDA3OTkwMjAsICJzb3J0X2J5IjogImlkLWRlc2MiLCAiY3Vyc29yX2RpciI6ICJuZXh0In"
}
records = []

stop_backfill = pd.to_datetime('2025-01-01T00:00:00+00:00')

# Paginate through the API endpoint and create a list of backfill records
while pd.to_datetime(params["updated_at_max"]) < stop_backfill:
    u_min= params["updated_at_min"]
    u_max= params["updated_at_max"]
    while True:
        response = airbud.get_data(url, headers, params, None, None)
        
        # Check for Rate Limiting or other errors
        if response.status_code != 200:
            response = airbud.retry_get_data(url, headers, params, None, None)
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
filename = f"get/recharge/{endpoint}/historical_072024.json"
blob = bucket.blob(filename)
blob.upload_from_string(json.dumps(records), content_type='application/json')


cursors = {
  #"01/01/25": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI1LTAxLTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAxMzgzNDcwMDQ5NiwgImxhc3RfdmFsdWUiOiAxMzgzNDcwMDQ5NiwgInNvcnRfYnkiOiAiaWQtZGVzYyIsICJjdXJzb3JfZGlyIjogIm5leHQiLCAib2JqZWN0X3R5cGUiOiAic3Vic2NyaXB0aW9uIn0=0=",
  #noevents "02/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTAyLTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiA5Njg0Mjc3MjY5LCAibGFzdF92YWx1ZSI6IDk2ODQyNzcyNjksICJzb3J0X2J5IjogImlkLWRlc2MiLCAiY3Vyc29yX2RpciI6ICJuZXh0IiwgIm9iamVjdF90eXBlIjogInN1YnNjcmlwdGlvbiJ90=",
  #"02/01/25": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI1LTAyLTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAwLCAibGFzdF92YWx1ZSI6IDAsICJzb3J0X2J5IjogImlkLWRlc2MiLCAiY3Vyc29yX2RpciI6ICJuZXh0IiwgIm9iamVjdF90eXBlIjogInN1YnNjcmlwdGlvbiJ90=",
  #noevents "03/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTAzLTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiA5OTkzMzYyNzM1LCAibGFzdF92YWx1ZSI6IDk5OTMzNjI3MzUsICJzb3J0X2J5IjogImlkLWRlc2MiLCAiY3Vyc29yX2RpciI6ICJuZXh0IiwgIm9iamVjdF90eXBlIjogInN1YnNjcmlwdGlvbiJ90=",
  #"04/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTA0LTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAxMDMyNTIyNTY0NSwgImxhc3RfdmFsdWUiOiAxMDMyNTIyNTY0NSwgInNvcnRfYnkiOiAiaWQtZGVzYyIsICJjdXJzb3JfZGlyIjogIm5leHQiLCAib2JqZWN0X3R5cGUiOiAic3Vic2NyaXB0aW9uIn0=",
  #"05/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTA1LTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAxMDY4OTM1MzUyNCwgImxhc3RfdmFsdWUiOiAxMDY4OTM1MzUyNCwgInNvcnRfYnkiOiAiaWQtZGVzYyIsICJjdXJzb3JfZGlyIjogIm5leHQiLCAib2JqZWN0X3R5cGUiOiAic3Vic2NyaXB0aW9uIn0=",
  #"06/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTA2LTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAxMTA2MjM0NDc0MCwgImxhc3RfdmFsdWUiOiAxMTA2MjM0NDc0MCwgInNvcnRfYnkiOiAiaWQtZGVzYyIsICJjdXJzb3JfZGlyIjogIm5leHQiLCAib2JqZWN0X3R5cGUiOiAic3Vic2NyaXB0aW9uIn0=",
  #"07/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTA3LTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAxMTQyNzg5ODUzNSwgImxhc3RfdmFsdWUiOiAxMTQyNzg5ODUzNSwgInNvcnRfYnkiOiAiaWQtZGVzYyIsICJjdXJzb3JfZGlyIjogIm5leHQiLCAib2JqZWN0X3R5cGUiOiAic3Vic2NyaXB0aW9uIn0=",
  #"08/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTA4LTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAxMTc4MDU2MDY0OCwgImxhc3RfdmFsdWUiOiAxMTc4MDU2MDY0OCwgInNvcnRfYnkiOiAiaWQtZGVzYyIsICJjdXJzb3JfZGlyIjogIm5leHQiLCAib2JqZWN0X3R5cGUiOiAic3Vic2NyaXB0aW9uIn0=",
  #"09/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTA5LTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAxMjE0MTIzMjc1MiwgImxhc3RfdmFsdWUiOiAxMjE0MTIzMjc1MiwgInNvcnRfYnkiOiAiaWQtZGVzYyIsICJjdXJzb3JfZGlyIjogIm5leHQiLCAib2JqZWN0X3R5cGUiOiAic3Vic2NyaXB0aW9uIn0=",
  #"10/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTEwLTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAxMjUyNTY5NjcwNiwgImxhc3RfdmFsdWUiOiAxMjUyNTY5NjcwNiwgInNvcnRfYnkiOiAiaWQtZGVzYyIsICJjdXJzb3JfZGlyIjogIm5leHQiLCAib2JqZWN0X3R5cGUiOiAic3Vic2NyaXB0aW9uIn0=",
  #"11/01/24": "eyJjcmVhdGVkX2F0X21pbiI6ICIyMDI0LTExLTAxVDAwOjAwOjAwWiIsICJzdGFydGluZ19iZWZvcmVfaWQiOiAxMjk0NjY5MzAyMywgImxhc3RfdmFsdWUiOiAxMjk0NjY5MzAyMywgInNvcnRfYnkiOiAiaWQtZGVzYyIsICJjdXJzb3JfZGlyIjogIm5leHQiLCAib2JqZWN0X3R5cGUiOiAic3Vic2NyaXB0aW9uIn0=",
}

records = []

cursor_date = "07/01/24"

for cursor in cursors.values():
    params = {
        "limit": 250,
        "cursor": cursors[cursor_date]
    }
    
    while True:
        response = airbud.get_data(url, headers, params, None, None)
        
        # Check for Rate Limiting or other errors
        if response.status_code != 200:
            response = airbud.retry_get_data(url, headers, params, None, None)
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
    
    print(len(records))

   df = pd.DataFrame(records)
   df.dtypes
   df.created_at.min()
   df.created_at.max()