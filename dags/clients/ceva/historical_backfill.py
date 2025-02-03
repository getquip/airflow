from custom_packages import airbud
import json
import boto3

# Create an S3 client
session = boto3.Session(
    aws_access_key_id="TEST",
    aws_secret_access_key="TEST"
)

s3_client = session.client('s3')

bucket_name = 'c5aa903e-2d4b-4853-b78c-af44763ec434'
dag_run_date = '2025-01-27 00:00:00+00:00'

# Initialize Google Clients
from google.cloud import storage, bigquery
gcs_client = storage.Client()
gcs_bucket = gcs_client.get_bucket('quip_airflow_dev')
bq_client = bigquery.Client()

# Use a paginator to handle large buckets
paginator = s3_client.get_paginator('list_objects_v2')

# Collect all object keys
keys = []
for page in paginator.paginate(Bucket=bucket_name, Prefix="processed/"):
    if 'Contents' in page:
        for obj in page['Contents']:
            keys.append(obj['Key'])

for item in keys[1:]:
    if item.endswith('.csv'):
        print(item)
        # 1. determine endpoint
        if item.startswith('processed/inventory_snapshot/Inventory_Snapshot'):
            endpoint = "inventory_snapshot"
        elif item.startswith('processed/inventory_transactions/Inventory_Transactions'):
            endpoint = "inventory_transactions"
        elif item.startswith('processed/pre_advices/Pre_Advices'):
            endpoint = "pre_advices"
        else:
            endpoint = "unknown"
        
        if endpoint != 'unknown':
            # 2. Download the file locally
            local_file = f"tmp/{item}"
            s3_client.download_file(bucket_name, item, local_file)
            
            # 3. Load CSV to GCS
            gcs_path = f"get/ceva/{endpoint}/raw"
            gcs_blob = gcs_bucket.blob(f"{gcs_path}/{item}")
            gcs_blob.upload_from_filename(f"tmp/{item}")
            
            # 4. Prep the file for BigQuery
            supplemental = item.split('.')[0]
            json_path = f"get/ceva/{endpoint}/DAG_RUN:{dag_run_date}/{supplemental}.json"
            records = airbud.clean_column_names(local_file, json_path, dag_run_date)
            
            # 5. Load JSON to GCS
            gcs_blob = gcs_bucket.blob(json_path)
            gcs_blob.upload_from_string(json.dumps(records), content_type='application/json')
            
            # 6. Load JSON to BigQuery
            table_ref = bq_client.dataset('ceva').table(endpoint)
            airbud.insert_records_to_bq(bq_client, table_ref, records)
            
            # 7. Move the file to the processed folder on S3
            response = s3_client.copy_object(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/{item}",
                Key=f"{endpoint}/processed/{item}"
            )
            
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                s3_client.delete_object(Bucket=bucket_name, Key=item)
            
    else:
        print('Not a CSV file')


# move gcs files (fix because of mis-targeted folders for gcs)
from google.cloud import storage
gcs_client = storage.Client()
gcs_bucket = gcs_client.get_bucket('quip_airflow_dev')
files = gcs_bucket.list_blobs(prefix=f'get/ceva/DAG_RUN:{dag_run_date}/')

for file in files:
    if 'Inventory_Snapshot' in file.name:
        endpoint = "inventory_snapshot"
    elif 'Inventory_Transaction' in file.name:
        endpoint = "inventory_transactions"
    destination_blob_name = f'get/ceva/{endpoint}/DAG_RUN:{dag_run_date}/{file.name.split("/")[-1]}'
    source_blob = gcs_bucket.blob(file.name)
    new_blob = gcs_bucket.copy_blob(source_blob, gcs_bucket, destination_blob_name)
    print(f"Moved to: {new_blob.name}")
    # Delete the original blob
    source_blob.delete()




from google.cloud import storage
gcs_client = storage.Client()
gcs_bucket = gcs_client.get_bucket('quip_airflow_dev')
files = gcs_bucket.list_blobs(prefix=f'get/ceva/raw/')

for file in files:
    if 'Inventory_Snapshot' in file.name:
        endpoint = "inventory_snapshot"
    elif 'Inventory_Transaction' in file.name:
        endpoint = "inventory_transactions"
    destination_blob_name = f'get/ceva/{endpoint}/raw/{file.name.split("/")[-1]}'
    source_blob = gcs_bucket.blob(file.name)
    new_blob = gcs_bucket.copy_blob(source_blob, gcs_bucket, destination_blob_name)
    print(f"Moved to: {new_blob.name}")
    # Delete the original blob
    source_blob.delete()

# Create table
from google.cloud import bigquery
import json
bq_client = bigquery.Client()
table_ref = bq_client.dataset('ceva').table('pre_advices')
with open('clients/ceva/schemas/pre_advices.json') as f:
    schema = json.load(f)
with open('clients/ceva/endpoints.json') as f:
    endpoints = json.load(f)
endpoint_kwargs = endpoints['pre_advices']
airbud.create_table_if_not_exists(
        bq_client,
        endpoint_kwargs,
        schema,
        table_ref
    ) 