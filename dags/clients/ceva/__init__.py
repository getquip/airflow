# Standard package imports
import os
import json
import pkgutil
import logging
import pandas as pd
from typing import List, Dict

# Third-party package imports
from airflow.models import TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Local package imports
from custom_packages import airbud

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

class GetCeva(airbud.GetClient):
    def __init__(
        self, 
        project_id: str, 
        bucket_name: str, 
        aws_conn_id: str
        ) -> None:
        # Define client's dataset name
        self.dataset = "ceva"

       # Initialize the parent class
        super().__init__(project_id, bucket_name, self.dataset) 

        # Inititalize S3 connection
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        self.s3_bucket_name = "c5aa903e-2d4b-4853-b78c-af44763ec434"
        
        # Initialize BigQuery tables if they doesn't exist
        self.endpoints = self.load_endpoints(f"{ self.dataset }/endpoints.json")
        self.schemas = self.load_endpoint_schemas(self.dataset, self.endpoints.keys())
        self.generate_bq_tables()

        log.info(f"Initialized GetCeva Client.")

    def get_files(
        self,
        endpoint: str,
        endpoint_kwargs: Dict,
        **kwargs
        )-> str:
        # Initialize paths
        gcs_path = f"get/{self.dataset}/{endpoint}"
        file_prefix = endpoint_kwargs.get("file_prefix", "")

        # Get list of unprocessed file paths from the S3
        new_file_paths = self.s3_hook.list_keys(bucket_name=self.s3_bucket_name, prefix=file_prefix)
        
        if len(new_file_paths) > 0:
            log.info(f"Found {len(new_file_paths)} files to process.")

            for source_file in new_file_paths:
                
                # Download file using S3Hook
                local_file = self.s3_hook.download_file(
                    key=source_file,
                    bucket_name=self.s3_bucket_name,
                    preserve_file_name=True
                )

                # Upload raw csv file to GCS
                filename = airbud.upload_csv_to_gcs(self.gcs_bucket, gcs_path, local_file)

                 # Get the GCS path for the file
                gcs_filename, dag_run_date = airbud.generate_json_blob_name(
                    self.dataset, endpoint, supplemental=filename, **kwargs)
                
                # Clean the column names and convert to JSON
                records = airbud.clean_column_names(local_file, dag_run_date, gcs_filename)

                # upload the cleaned records to GCS
                
                airbud.upload_json_to_gcs(self.gcs_bucket, gcs_filename, records, **kwargs)

            # Push list of new GCS files to XCom
            task_instance = kwargs['task_instance']
            task_instance.xcom_push(key='s3_files', value=new_file_paths)
            log.info(f"Stored file names for {endpoint} in XComs: {new_file_paths}")
            return "success"
        # else:
        #     log.info(f"No new files found in bucket")
        #     return "no_new_files"

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
        
        if return_value == "success":
            # Get the file names from XCom
            files = task_instance.xcom_pull(task_ids=upstream_task, key='s3_files')

            files_to_move, bad_files = airbud.insert_files_to_bq(
                files,
                endpoint,
                self.dataset,
                self.bq_client,
                self.gcs_bucket,
                **kwargs
                )

            # Push list of new GCS files to XCom
            task_instance = kwargs['task_instance']
            task_instance.xcom_push(key='files_to_move', value=files_to_move)
            task_instance.xcom_push(key='bad_files', value=[])
        else:
            log.info("Do Nothing.")

        """Move files in SFTP to processed folder."""
    def move_to_processed(
        self,
        endpoint: str,
        **kwargs
        ) -> str:
        # Check upstream task
        task_instance = kwargs['ti']
        upstream_task = f'get__{ endpoint }.load_{ endpoint }_files_to_bq'
        files_to_move = task_instance.xcom_pull(task_ids=upstream_task, key='files_to_move')
        bad_files = task_instance.xcom_pull(task_ids=upstream_task, key='bad_files')

        if files_to_move or bad_files:
            if len(files_to_move) > 0:
                for source_file in files_to_move:
                    local_file = os.path.basename(source_file)
                    processed_path = f"processed/{endpoint}/{local_file}"
                    try:
                        airbud.move_files_on_s3(self.s3_hook, self.s3_bucket_name, local_file, processed_path)
                    except Exception as e:
                        log.error(f"Error moving {source_file} to processed: {e}")
                        bad_files.append(source_file)
            elif len(bad_files) == 0 and len(files_to_move) == 0:
                log.info("Do Nothing.")

            # raise error if there are any bad files
            if len(bad_files) > 0:
                raise Exception(f"Failed to fully process { len(bad_files) } files: {bad_files}")
            else:
                return "success"