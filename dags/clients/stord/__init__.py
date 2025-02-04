# Standard package imports
import os
import json
import pkgutil
import logging
import pandas as pd
from typing import List, Dict

# Third-party package imports
from airflow.models import TaskInstance

# Local package imports
from custom_packages import airbud

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class GetStord(airbud.GetClient):

    """Initialize the GetStord class."""
    def __init__(
        self, 
        project_id: str, 
        bucket_name: str, 
        sftp_conn_id: str
        ) -> None:
        # Define client's dataset name
        self.dataset = "stord"

        # Initialize the parent class
        super().__init__(project_id, bucket_name, self.dataset) 

        # Define the parent path
        self.sftp_conn_id = sftp_conn_id
        
        # Initialize BigQuery tables if they doesn't exist
        self.endpoints = self.load_endpoints(f"{ self.dataset }/endpoints.json")
        self.schemas = self.load_endpoint_schemas(self.dataset, self.endpoints.keys())
        self.generate_bq_tables()

    """Get files from SFTP, clean, and upload to GCS."""
    def get_files(
        self,
        endpoint: str,
        endpoint_kwargs: Dict,
        **kwargs
        )-> str:
        # Initialize paths
        path = endpoint_kwargs.get('path')
        sftp_path = f"{path}/{endpoint}"
        gcs_path = f"get/{self.dataset}/{endpoint}"

        # Get list of unprocessed file paths from the SFTP
        remote_files = airbud.list_sftp_files(self.sftp_conn_id, sftp_path, endpoint_kwargs)

        if len(remote_files) > 0:
            log.info(f"Found {len(remote_files)} files in {sftp_path}")
                
            # Process new files
            for source_file in remote_files:
                log.info(f"Processing {source_file}...")

                # Download the files from SFTP files
                local_file = airbud.download_sftp_file(self.sftp_conn_id, source_file)

                # open any zipped files
                if local_file.endswith('.zip'):
                    log.info("Unzipping file...")
                    csv_files = airbud.unzip_files(local_file)
                    log.info(f"Found {len(csv_files)} csv files in the zip file.")
                    for csv_file in csv_files:
                        airbud.load_files_to_gcs(
                            self.gcs_bucket,
                            gcs_path,
                            self.dataset,
                            endpoint,
                            csv_file,
                            **kwargs
                            )
                else:
                    airbud.load_files_to_gcs(
                            self.gcs_bucket,
                            gcs_path,
                            self.dataset,
                            endpoint,
                            local_file,
                            **kwargs
                            )
            # Push list of new GCS files to XCom
            task_instance = kwargs['task_instance']
            task_instance.xcom_push(key='sftp_files', value=remote_files)
            log.info(f"Stored file names for {endpoint} in XComs: {remote_files}")
            return "success"
        else:
            log.info(f"No new files found in {sftp_path}")
            return "no_new_files"

    """Load files from GCS to BigQuery."""
    def load_to_bq(
        self,
        endpoint: str,
        endpoint_kwargs: dict,
        **kwargs
        ) -> str:
        # Check upstream task
        task_instance = kwargs['ti']
        upstream_task = f'get__{ endpoint }.ingest_{ endpoint }_files'
        return_value = task_instance.xcom_pull(task_ids=upstream_task, key='return_value')
        print(return_value)
        if return_value == "success":
            # Get the file names from XCom
            sftp_files = task_instance.xcom_pull(task_ids=upstream_task, key='sftp_files')

            files_to_move, bad_files = airbud.insert_files_to_bq(
                sftp_files,
                endpoint,
                self.dataset,
                self.bq_client,
                self.gcs_bucket,
                **kwargs
                )
            
            # Push list of new GCS files to XCom
            task_instance = kwargs['task_instance']
            task_instance.xcom_push(key='files_to_move', value=files_to_move)
            task_instance.xcom_push(key='bad_files', value=bad_files)
        else:
            log.info("Do Nothing.")

    """Move files in SFTP to processed folder."""
    def move_to_processed(
        self,
        endpoint: str,
        endpoint_kwargs:str,
        **kwargs
        ) -> str:
        # Check upstream task
        task_instance = kwargs['ti']
        upstream_task = f'get__{ endpoint }.load_{ endpoint }_files_to_bq'
        files_to_move = task_instance.xcom_pull(task_ids=upstream_task, key='files_to_move')
        bad_files = task_instance.xcom_pull(task_ids=upstream_task, key='bad_files')

        if files_to_move:
            if len(files_to_move) > 0:
                for source_file in files_to_move:
                    path = endpoint_kwargs.get('path')
                    processed_path = f"{path}/{endpoint}/processed"
                    try:
                        log.info(f"Moving {source_file} to {processed_path}")
                        airbud.move_file_on_sftp(self.sftp_conn_id, source_file, processed_path)
                    except Exception as e:
                        log.error(f"Error moving {source_file} to processed: {e}")
                        bad_files.append(source_file)
            elif len(bad_files) == 0 and len(files_to_move) == 0:
                log.info("Do Nothing.")
        
            # raise error if there are any bad files
            if len(bad_files) > 0:
                raise Exception(f"Failed to fully process { len(bad_files) } files: {bad_files}")
        return "success"
        
