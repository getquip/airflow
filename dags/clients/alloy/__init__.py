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

class GetAlloy(airbud.GetClient):
    def __init__(
        self, 
        project_id: str, 
        bucket_name: str, 
        aws_conn_id: str,
        ) -> None:
        # Define client's dataset name
        self.dataset = "alloy"

       # Initialize the parent class
        super().__init__(project_id, bucket_name, self.dataset) 

        # Inititalize Third-party GCS bucket
        self.alloy_bucket = gcs_client.get_bucket('alloy_exports_v161')
        self.gcs_client = storage.Client()
        self.gcs_bucket = self.gcs_client.get_bucket(bucket_name)
        
        # Initialize BigQuery tables if they doesn't exist
        self.endpoints = self.load_endpoints(f"{ self.dataset }/endpoints.json")
        self.schemas = self.load_endpoint_schemas(self.dataset, self.endpoints.keys())
        self.generate_bq_tables()

        log.info(f"Initialized GetAlloy Client.")

    def get_files(
        self,
        endpoint: str,
        endpoint_kwargs: Dict,
        **kwargs
        )-> str:
        # Initialize paths
        gcs_path = f"get/{self.dataset}/{endpoint}"

        # Get list of unprocessed csv files
		new_files = airbud.list_all_blobs(alloy_bucket_name, endpoint)
        
        
        if len(new_files) > 0:
            log.info(f"Found {len(new_files)} files to process.")

            for source_blob in new_files:
                # Copy the blob to the destination bucket
    			source_bucket.copy_blob(source_blob, self.gcs_bucket, source_blob.name)

                # Generate the destination blob name
				filename_no_file_type = source_blob.name.split(".")[0]
				json_filename, dag_run_date = generate_json_blob_name(
					self.dataset, endpoint, supplemental=filename_no_file_type, **kwargs)

				# Download the blob as a string
   				csv_data = blob.download_as_text()

				# Use appropriate reader based on the file type (CSV in this case)
				df = load_csv_to_df(csv_data)

				# Clean the column names and convert to JSON
				records = airbud.clean_column_names(source_file, json_filename, dag_run_date)
				
				# Upload the JSON data to GCS
				upload_json_to_gcs(gcs_bucket, json_filename, records)

            # Push list of new GCS files to XCom
            task_instance = kwargs['task_instance']
            task_instance.xcom_push(key='s3_files', value=new_file_paths)
            log.info(f"Stored file names for {endpoint} in XComs: {new_file_paths}")
            return "success"
        else:
            log.info(f"No new files found in bucket")
            return "no_new_files"

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
                    destination_blob_name = f"{endpoint}/processed/{source_file}"
                    try:
                        airbud.move_file_in_gcs(self.alloy_bucket, source_file, destination_blob_name)
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
