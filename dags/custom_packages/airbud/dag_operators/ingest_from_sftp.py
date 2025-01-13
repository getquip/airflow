# Standard library imports
import logging 

# Third-party imports
from airflow.providers.sftp.hooks.sftp import SFTPHook
from google.cloud import bigquery
from google.cloud import storage

# Local package imports
from custom_packages.airbud.sftp import *
from custom_packages.airbud.gcs import upload_csv_to_gcs
from custom_packages.airbud.post_to_bq import insert_records

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def ingest_from_sftp(
    project_id: str,
    bucket_name: str,
    sftp_conn_id: str,
    endpoint: str,
    client: object,
    endpoint_kwargs: dict,
    **kwargs
) -> None:
    # Initialize paths
    sftp_path = f"{client.parent_path}/{endpoint}"
    gcs_path = f"get/{client.dataset}/{endpoint}"
    
    # Initialize connections
    sftp_hook = SFTPHook(ssh_conn_id=sftp_conn_id)
    bq_client = bigquery.Client(project=project_id)
    gcs_client = storage.Client(project=project_id)
    
    # Get list of unprocessed files from the SFTP
    sftp_files = list_sftp_files(sftp_hook, sftp_path)

    if len(new_files) > 0:
        log.info(f"Found {len(sftp_files)} files in {sftp_path}")
       
        # Download the files from SFTP files
        log.info(f"Downloading {len(new_files)} files from SFTP...")
        download_sftp_files(sftp_hook, sftp_path, new_files)

        # Process new files
        for file in new_files:
            log.info(f"Processing {file}...")
            # Clean the column names and convert to JSON for BQ insertion
            records = clean_column_names(file)
            
            # Insert the records to BigQuery
            try:
                table_ref = get_destination(bq_client, client, endpoint, endpoint_kwargs)
                insert_records(bq_client, table_ref, records)
                # Upload the files to GCS
                upload_csv_to_gcs(gcs_client, bucket_name, gcs_path, file)
                move_file_on_sftp(sftp_hook, sftp_path, file)
                log.info(f"Successfully uploaded {file} to BigQuery and GCS.")
            except Exception as e:
                # If failed to insert to BigQuery, store the file in error folder of the endpoint
                log.warning(f"Error uploading {filename} to BigQuery: {e}")
                dag_run: DagRun = kwargs.get('dag_run')
                dag_run_date = dag_run.execution_date
                error_file_path = f"{gcs_path}/error/{dag_run_date}"
                upload_csv_to_gcs(gcs_client, bucket_name, error_file_path, file)
    else:
        log.info("No new files to process.")