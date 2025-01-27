import paramiko
import csv
from custom_packages import airbud
import logging
import os
import re
import json

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# SFTP server details
hostname = 'ftp.ful.stord.com'
port = 22
username = 'Quip'
password = "Rn.ieVq252"

# Create an SSH client
ssh = paramiko.SSHClient()

# Automatically add the server's host key (INSECURE - use with caution)
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Connect to the SSH server
ssh.connect(hostname, port, username, password)

# Open an SFTP session
sftp = ssh.open_sftp()

# Get endpoint
endpoint = 'parcel_fulfillment_invoices'
dag_run_date = '2025-01-27 00:00:00+00:00'
# Get GCS bucket
from google.cloud import storage, bigquery
gcs_client = storage.Client()
gcs_bucket = gcs_client.get_bucket('quip_airflow_dev')
bq_client = bigquery.Client()

# List files in the remote directory
remote_directory = f'billing/{endpoint}'
print(f"Listing files in {remote_directory}:")

remote_files = []
for filename in sftp.listdir(remote_directory):
    remote_files.append(filename)

print(f"Found {len(remote_files)} remote files.")

bad_files = []

for file in remote_files:
    # 1. Download the file from the SFTP server
    source_file = f"{remote_directory}/{file}"
    sftp.get(source_file, file)
    local_file = file
    
    # 1.1. Unzip file if necessary
    if local_file.endswith('.zip'):
        log.info("Unzipping file...")
        csv_files = unzip_files(local_file)
        
        for csv_file in csv_files:
            # 2. Upload the file(s) to Google Cloud Storage (GCS)
            if '.csv.csv' in csv_file:
                csv_file = csv_file.replace('.csv.csv', '.csv')	
            
            gcs_csv_path = f"get/{endpoint}/raw/{csv_file}"
            
            # Upload the file to GCS
            blob = gcs_bucket.blob(gcs_csv_path)
            blob.upload_from_filename(local_file)
            log.info(f"Uploaded to csv to GCS: {gcs_csv_path}")
            
            # 3. Prepare the file for BigQuery
            supplemental = csv_file.split('.')[0]
            json_file_path = f"get/{endpoint}/DAG_RUN:{dag_run_date}/{supplemental}.json"
            records = clean_column_names(csv_file, json_file_path, dag_run_date)
            
            if len(records) > 0:
                # 4. Load the file as JSON records to GCS
                blob = gcs_bucket.blob(json_file_path)
                blob.upload_from_string(json.dumps(records), content_type='application/json')
                log.info(f"Uploaded json data to GCS: {filename}")
                
                # 5. Insert the records to BigQuery
                try:
                    table_ref = bq_client.dataset('stord').table(endpoint)
                    
                    airbud.insert_records_to_bq(bq_client, table_ref, records)
                    log.info(f"Successfully uploaded {local_file} to BigQuery.")
                    
                    # 6. Move the file to the processed directory
                    destination_path = f"{remote_directory}/processed/{file}"
                    sftp.rename(source_file, destination_path)
                    log.info(f"Moved {source_file} to {destination_path}")
                except Exception as e:
                    log.error(f"Error uploading {local_file} to BigQuery: {e}")
                    bad_files.append(file)
    else:
        # 2. Upload the file to Google Cloud Storage (GCS)
        if '.csv.csv' in local_file:
            local_file = local_file.replace('.csv.csv', '.csv')	
            
        gcs_csv_path = f"get/{endpoint}/raw/{local_file}"
        
        # Upload the file to GCS
        blob = gcs_bucket.blob(gcs_csv_path)
        blob.upload_from_filename(local_file)
        log.info(f"Uploaded to csv to GCS: {blob.name}")
        
        # 3. Prepare the file for BigQuery
        supplemental = local_file.split('.')[0]
        json_file_path = f"get/{endpoint}/DAG_RUN:{dag_run_date}/{supplemental}.json"
        records = clean_column_names(local_file, json_file_path, dag_run_date)
        
        # 4. Load the file as JSON records to GCS
        blob = gcs_bucket.blob(json_file_path)
        blob.upload_from_string(json.dumps(records), content_type='application/json')
        log.info(f"Uploaded json data to GCS: {filename}")
        
        # 5. Insert the records to BigQuery
        try:
            table_ref = bq_client.dataset('stord').table(endpoint)
            
            airbud.insert_records_to_bq(bq_client, table_ref, records)
            log.info(f"Successfully uploaded {local_file} to BigQuery.")
            
            # 6. Move the file to the processed directory
            destination_path = f"{remote_directory}/processed/{file}"
            sftp.rename(source_file, destination_path)
        except Exception as e:
            log.error(f"Error uploading {local_file} to BigQuery: {e}")
            bad_files.append(file)