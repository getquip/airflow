# Standard package imports
import os
import json
import pkgutil
import logging
import pandas as pd
from typing import List, Dict

# Third-party package imports
from google.cloud import storage, bigquery

# Local package imports
from custom_packages import airbud

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class GetWenParker(airbud.GetClient):
    def __init__(self, project_id: str, bucket_name: str, sftp_conn_id: str):
        # Define dataset name
        self.dataset = "wen_parker"

        # Initialize the parent class
        super().__init__(project_id, bucket_name, self.dataset) 

        # Define the parent path
        self.parent_path = "Quip/wen_parker"
        self.sftp_conn_id = sftp_conn_id
        
        # Initialize BigQuery tables if they doesn't exist
        self.endpoints = self.load_endpoints(f"{ self.dataset }/endpoints.json")
        self.schemas = self.load_endpoint_schemas(self.dataset, self.endpoints.keys())
        self.generate_bq_tables()

        log.info(f"Initialized GetWenParker with parent path: {self.parent_path}")

    def get_files(
        self,
        endpoint: str,
        endpoint_kwargs: Dict,
        **kwargs
    )-> str:
        # Initialize paths
        sftp_path = f"{self.parent_path}/{endpoint}"
        gcs_path = f"get/{self.dataset}/{endpoint}"

        # Get list of unprocessed file paths from the SFTP
        new_file_paths = airbud.list_sftp_files(self.sftp_conn_id, sftp_path, endpoint_kwargs)

        if len(new_file_paths) > 0:
            log.debug(f"Found {len(new_file_paths)} files in {sftp_path}")
            
            # Download the files from SFTP files
            log.info(f"Downloading files from SFTP...")
            airbud.download_sftp_files(self.sftp_conn_id, new_file_paths)

            # Process new files
            for source_file in new_file_paths:
                local_file = os.path.basename(source_file)
                log.info(f"Processing {source_file}...")
                
                # Upload raw csv file to GCS
                airbud.upload_csv_to_gcs(self.gcs_client, self.bucket_name, gcs_path, local_file)

                # Clean the column names and convert to JSON
                records = airbud.clean_column_names(local_file, **kwargs)
                
                # upload the cleaned records to GCS
                airbud.upload_json_to_gcs(self.gcs_client, self.bucket_name, self.dataset, endpoint, records, **kwargs)

            # Push list of new GCS files to XCom
            task_instance = kwargs['task_instance']
            task_instance.xcom_push(
                key='sftp_files',
                value=new_file_paths
            )
            log.info(f"Stored file names for {endpoint} in XComs: {new_file_paths}")
            return "success"

    def load_to_bq(
        self,
        endpoint: str,
        endpoint_kwargs: dict,
        **kwargs
    ) -> str:
        # Get the file names from XCom
        task_instance = kwargs['ti']
        upstream_task = f'get__{ endpoint }.ingest_{ endpoint }_files'
        sftp_files = task_instance.xcom_pull(task_ids=upstream_task, key='sftp_files')

        files_to_move = []
        bad_files = []
        # Process each file
        for source_file in sftp_files:
            log.info(f"Processing {source_file}...")
            # Get file names
            local_file = os.path.basename(source_file)

            try: # Get records from GCS
                records = airbud.get_records_from_file(
                    self.gcs_client, 
                    self.bucket_name, 
                    self.dataset, 
                    endpoint,
                    **kwargs
                )
                log.debug(f"Successfully loaded {len(records)} records from GCS.")
            except Exception as e:
                log.error(f"Failed to get records from file: {e}")
                bad_files.append(local_file)
                records = []

            if len(records) > 0:
                try: # Upload records to BigQuery
                    # Get or create the BigQuery table
                    table_ref = self.bq_client.dataset(self.dataset).table(endpoint)

                    # Insert the records to BigQuery
                    airbud.insert_records_to_bq(self.bq_client, table_ref, records)
                    log.debug(f"Successfully uploaded {local_file} to BigQuery.")
                    files_to_move.append(source_file)
                except Exception as e:
                    log.error(f"Error uploading {local_file} to BigQuery: {e}")
                    bad_files.append(local_file)

        if len(bad_files) > 0:
            log.error(f"Failed to process { len(bad_files) } files: {bad_files}")
        if len(files_to_move) > 0:
            log.info(f"Successfully uploaded { len(files_to_move) } files to BigQuery.")
        
        # Push list of new GCS files to XCom
        log.info(f"Saving list of files to move to XComs...")
        task_instance = kwargs['task_instance']
        task_instance.xcom_push(
            key='files_to_move',
            value=files_to_move
        )
        task_instance.xcom_push(
            key='bad_files',
            value=bad_files
        )
        return "success"

    
    def move_to_processed(
        self,
        endpoint: str,
        **kwargs
    ) -> str:
        # Get the file names from XCom
        task_instance = kwargs['ti']
        upstream_task = f'get__{ endpoint }.load_{ endpoint }_files_to_bq'
        files_to_move = task_instance.xcom_pull(task_ids=upstream_task, key='files_to_move')
        bad_files = task_instance.xcom_pull(task_ids=upstream_task, key='bad_files')

        if len(files_to_move) > 0:
            for source_file in files_to_move:
                processed_path = f"{self.parent_path}/{endpoint}/processed"
                try:
                    log.info(f"Moving {source_file} to {processed_path}...")
                    airbud.move_file_on_sftp(self.sftp_conn_id, source_file, processed_path)
                    return "success"
                except Exception as e:
                    log.error(f"Error moving {source_file} to processed: {e}")
                    bad_files.append(source_file)
        else:
            log.info(f"No files to move for {endpoint}.")
        
        # raise error if there are any bad files
        if len(bad_files) > 0:
            raise Exception(f"Failed to fully process { len(bad_files) } files: {bad_files}")
	