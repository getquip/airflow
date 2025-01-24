from custom_packages import airbud
from google.cloud import storage, bigquery
import os
import json 
import logging


# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

client = storage.Client()
gcs_bucket = client.get_bucket('quip_airflow_dev')

files = airbud.list_all_files(gcs_bucket, 'get/ceva/inventory_snapshot/DAG_RUN:2025-01-22 17:46:35.465240+00:00')

bq_client = bigquery.Client()
dataset = 'ceva'
endpoint = 'inventory_snapshot'
table_ref = bq_client.dataset(dataset).table(endpoint)
with open('clients/ceva/schemas/inventory_snapshot.json') as f:
    schema = json.load(f)
with open('clients/ceva/endpoints.json') as f:
    endpoints = json.load(f)
endpoint_kwargs = endpoints[endpoint]
airbud.create_table_if_not_exists(bq_client,endpoint_kwargs,schema,table_ref)
files_to_move = []
bad_files = []
# Process each file
for source_file in files[0:]:
    # Get file name
    local_file = os.path.basename(source_file)
    log.info(f"Processing {source_file}...")
    try: 
        records = airbud.get_records_from_file(gcs_bucket, source_file)
        log.info(f"Successfully loaded {len(records)} records from GCS.")
    except Exception as e:
        log.error(f"Failed to get records from file: {e}")
        bad_files.append(local_file)
        records = []
    if len(records) > 0:
        try: # Upload records to BigQuery
            # Insert the records to BigQuery
            airbud.insert_records_to_bq(bq_client, table_ref, records)
            log.info(f"Successfully uploaded {local_file} to BigQuery.")
            files_to_move.append(local_file)
        except Exception as e:
            log.error(f"Error uploading {local_file} to BigQuery: {e}")
            bad_files.append(local_file)

## rename blobs in gcs
for blob_name in files:
    if '.csv' in blob_name:
        # check if blob exists as json
        json_blob_name = blob_name.replace('.csv','')
        json_blob = gcs_bucket.blob(json_blob_name)
        source_blob = gcs_bucket.blob(blob_name)
        if blob.exists():
            source_blob.delete()
        else:
            source_blob = gcs_bucket.blob(blob_name)
    # # Copy the blob to the new name
    # new_blob_name = blob_name.replace('.csv','')
    # new_blob = gcs_bucket.blob(new_blob_name)
    # new_blob.rewrite(source_blob)
    # Delete the original blob
    source_blob.delete()