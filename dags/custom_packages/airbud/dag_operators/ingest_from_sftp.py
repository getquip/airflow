# Standard library imports
import logging 
import os

# Third-party imports
from airflow.providers.sftp.hooks.sftp import SFTPHook
from google.cloud import bigquery
from google.cloud import storage

# Local package imports
from custom_packages.airbud import sftp
from custom_packages.airbud import gcs
from custom_packages.airbud import post_to_bq

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
    bq_client = bigquery.Client(project=project_id)
    gcs_client = storage.Client(project=project_id)
    
    # Get list of unprocessed file paths from the SFTP
    new_file_paths = sftp.list_sftp_files(sftp_conn_id, sftp_path, endpoint_kwargs)

    if len(new_file_paths) > 0:
        log.info(f"Found {len(new_file_paths)} files in {sftp_path}")
       
        # Download the files from SFTP files
        log.info(f"Downloading files from SFTP...")
        sftp.download_sftp_files(sftp_conn_id, new_file_paths)

        # Process new files
        bad_files = []
        for source_file in new_file_paths:
            local_file = os.path.basename(source_file)
            log.info(f"Processing {source_file}...")
            # Clean the column names and convert to JSON for BQ insertion
            records = sftp.clean_column_names(local_file, **kwargs)
            
            # Insert the records to BigQuery
            try:
                table_ref = post_to_bq.get_destination(bq_client, client, endpoint, endpoint_kwargs)
                post_to_bq.insert_records(bq_client, table_ref, records)
                # Upload the files to GCS
                gcs.upload_csv_to_gcs(gcs_client, bucket_name, gcs_path, local_file)
                sftp.move_file_on_sftp(sftp_conn_id, source_file, local_file, sftp_path)
                log.info(f"Successfully uploaded {local_file} to BigQuery and GCS.")
            except Exception as e:
                # If failed to insert to BigQuery, store the file in error folder of the endpoint
                log.warning(f"Error uploading {local_file} to Quip Environment: {e}")
                dag_run: DagRun = kwargs.get('dag_run')
                dag_run_date = dag_run.execution_date
                error_file_path = f"{gcs_path}/error/{dag_run_date}"
                gcs.upload_csv_to_gcs(gcs_client, bucket_name, error_file_path, local_file)
                bad_files.append(source_file)
        if len(bad_files) > 0:
            raise Exception(f"Failed to process {len(bad_files)} files: {bad_files}")
    else:
        log.info("No new files to process.")