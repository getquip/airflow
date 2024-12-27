# this file is for a manual backfill since Airflow cannot backfill the size of data
from custom_packages import airbud
from typing import List, Dict
import pandas as pd
import json
from clients.recharge.paginate import paginate_responses

# Define constants for data source
PROJECT_ID = "quip-dw-raw"
DATA_SOURCE_NAME = "recharge"
BASE_URL = "https://api.rechargeapps.com/"
INGESTION_METADATA = {
    "project_id": PROJECT_ID,
    "dataset_name": DATA_SOURCE_NAME,
    "base_url": BASE_URL,
    "gcs_bucket_name": 'quip_airflow'
}

# Update headers with API key
SECRET_PREFIX = "api__"
API_KEY = airbud.get_secrets(PROJECT_ID, DATA_SOURCE_NAME, SECRET_PREFIX)
INGESTION_METADATA["headers"] = {
    "X-Recharge-Access-Token": API_KEY['api_key'],
    "X-Recharge-Version": "2021-11"
}

# Postman Collection: https://quipdataeng.postman.co/workspace/quip_data_eng~9066eadd-c088-4794-8fc6-2774ed80218c/collection/39993065-1e67e281-3311-4b9e-b207-d5154c7339cf?action=share&creator=39993065
with open("clients/recharge/endpoint_kwargs.json", "r") as file:
	ENDPOINT_KWARGS = json.load(file)


for endpoint in ENDPOINT_KWARGS.keys():
    print(endpoint)
    airbud.ingest_data(
        ingestion_metadata = INGESTION_METADATA,
        endpoint = endpoint,  # API endpoint
        endpoint_kwargs = ENDPOINT_KWARGS[endpoint], # Endpoint-specific arguments
        paginate=True,    # Initialize pagination flag
        pagination_function=paginate_responses,  # Initialize pagination arguments
    )



endpoint = 'charges'
records = paginate_responses(endpoint, BASE_URL + endpoint, INGESTION_METADATA["headers"], {})
records = airbud.post_to_gcs.upload_json_to_gcs(PROJECT_ID, records, INGESTION_METADATA["gcs_bucket_name"], f"get/{DATA_SOURCE_NAME}/{endpoint}", ENDPOINT_KWARGS[endpoint]['destination_blob_name'])

i = 11000
while True:
    try:
        airbud.post_to_bq.upload_to_bigquery(PROJECT_ID, DATA_SOURCE_NAME, endpoint, ENDPOINT_KWARGS[endpoint]['bigquery_metadata'], records[i:i+1000])
        i += 1000
    except Exception as e:
        print(i)
        break