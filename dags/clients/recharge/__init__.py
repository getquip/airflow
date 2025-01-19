# Standard package imports
import json
import pkgutil
import logging
import pandas as pd
from typing import List, Dict

# Third-party package imports
from airflow.models import TaskInstance
from google.cloud import storage, bigquery
from airflow.exceptions import AirflowSkipException

# Local package imports
from custom_packages import airbud

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

class GetRecharge(airbud.GetClient):
    def __init__(self, project_id: str, bucket_name: str):
        self.dataset = "recharge"
        self.base_url = "https://api.rechargeapps.com/"

        # Initialize the parent class
        super().__init__(project_id, bucket_name, self.dataset) 

        # Get Recharge API credentials
        api_key = airbud.get_secrets(self.project_id, self.dataset, prefix="api__")
        self.headers = {
            "X-Recharge-Access-Token": api_key['api_key'],
            "X-Recharge-Version": "2021-11",
        }

        # Initialize BigQuery tables if they doesn't exist
        self.endpoints = self.load_endpoints(f"{ self.dataset }/endpoints.json")
        self.schemas = self.load_endpoint_schemas(self.dataset, self.endpoints.keys())
        self.generate_bq_tables()

    def paginate_responses(
        self,
        endpoint: str,  # The API endpoint
        endpoint_kwargs: dict,  # Endpoint-specific arguments
        url: str,  # The URL of the API endpoint
        **kwargs,
    ) -> List[Dict]:
        # API Documentation: https://developer.rechargepayments.com/2021-11/cursor_pagination

        # Set response limit to 250 (default is 50)
        params = endpoint_kwargs.get("params", {})
        params["limit"] = 250

        # Get last bookmark, None if no bookmark
        last_ts = airbud.get_last_page_from_last_dag_run(f"{self.dataset}__{endpoint}")
        
        # Only fetch the next day of data
        if last_ts:
            last_ts = pd.to_datetime(last_ts)
            stop_at = last_ts + pd.Timedelta(days=1)
            if endpoint == "events":
                params["created_at_min"] = last_ts
            else:
                params["updated_at_min"] = last_ts
            # If last_ts is today, do not pass the updated_at_max parameter
            if last_ts.date() == pd.Timestamp.utcnow().normalize().date() or stop_at.date() == pd.Timestamp.utcnow().normalize().date():
                pass
            # If stop_at date is today, do not pass the updated_at_max parameter
            elif stop_at.date() == pd.Timestamp.utcnow().normalize().date():
                pass
            else:
                if endpoint == "events":
                    params["created_at_max"] = stop_at
                else:
                    params["updated_at_max"] = stop_at
        else:
            last_ts = pd.to_datetime('2024-06-20')
            stop_at = pd.to_datetime('2024-06-21')
            params["updated_at_min"] = last_ts
            params["updated_at_max"] = stop_at
        log.info(f"Fetching data from {last_ts} to {stop_at}")

        # Paginate through the API endpoint and create a list of records
        records = []
        while True:
            response = airbud.get_data(url, self.headers, params, None, None)
            
            # Check for Rate Limiting or other errors
            if response.status_code != 200:
                response = airbud.retry_get_data(url, self.headers, params, None, None)
            if response.status_code == 200:
                # Parse response for records and append to records list
                response_json = response.json()
                records.extend(response_json.get(endpoint))
                
                # Check if there is another page of data
                next_page = response_json.get("next_cursor")
                if next_page:
                    # Only pass cursor as params
                    log.debug(f"Fetching next page of data...{next_page}")
                    params = {"cursor": next_page}
                else:
                    log.debug("No more data to fetch.")
                    break
            else:
                log.debug(f"Pagination halted due to status code: {response.status_code}")
                break
            
        # Pass bookmark for next run
        if len(records) > 0:
            df = pd.DataFrame(records)
            if endpoint == "events":
                df_max = df["created_at"].max()
            else:
                df_max = df["updated_at"].max()
            next_page = str(max(pd.to_datetime(df_max), stop_at))
        else:
            next_page = str(stop_at)
        return records, next_page

    def ingest_data(
        self,
        endpoint: str,  # API endpoint
        endpoint_kwargs: dict,  # Endpoint-specific arguments
        **kwargs
    ) -> str:

        # API Endpoint parameters
        url = self.base_url + endpoint

        # Get data
        log.info(f"Ingesting data from {endpoint} endpoint.")
        records, next_page = self.paginate_responses(endpoint, endpoint_kwargs, url, **kwargs)
        log.info(f"Completed data fetch for {endpoint}...")

        # Store next page as XComs for downstream tasks
        task_instance = kwargs['task_instance']
        task_instance.xcom_push(
            key='next_page',
            value=next_page
        )
        log.info(f"Stored next page for {endpoint} in XComs: {next_page}")

        # Upload data to GCS
        if len(records) > 0:
            log.info(f"Uploading {len(records)} records to GCS...")
            airbud.upload_json_to_gcs(
                self.gcs_client,
                self.bucket_name,
                self.dataset,
                endpoint,
                records,
                **kwargs 
            )
            return "success"

        else:
            return f"No records to upload."

    def load_to_bq(
        self,
        endpoint: str,
        endpoint_kwargs: dict,
        **kwargs  # Additional keyword arguments
        ) -> str:
        # Check for records
        task_instance = kwargs['ti']
        upstream_task = f'get__{ endpoint }.ingest_{ endpoint }_data'
        return_value = task_instance.xcom_pull(task_ids=upstream_task, key='return_value')
        if return_value == "success":
            # Get records from file or API
            try:
                records = airbud.get_records_from_file(
                    self.gcs_client, 
                    self.bucket_name, 
                    self.dataset, 
                    endpoint,
                    **kwargs
                )
                log.debug(f"Successfully loaded {len(records)} records from GCS.")
            except Exception as e:
                raise Exception(f"Failed to get records from file or it doesn't exist: { e }")
            
            table_ref = self.bq_client.dataset(self.dataset).table(endpoint)

            # Insert rows into BigQuery in chunks
            chunk_size = endpoint_kwargs.get("chunk_size", 8000)
            airbud.insert_records_to_bq(self.bq_client, table_ref, records, max_retries=3, chunk_size=chunk_size)
            log.info(f"Successfully inserted { len(records) } rows into { endpoint }.")

            # Get the next_page from XComs (from the upstream task)
            task_instance = kwargs['ti']
            upstream_task = f'get__{ endpoint }.ingest_{ endpoint }_data'
            next_page = task_instance.xcom_pull(task_ids=upstream_task, key='next_page')

        else:
            log.info(f"Skipping { endpoint } BigQuery task...")
        # Store bookmark for endpoint
        airbud.store_next_page_across_dags(self.dataset, endpoint, next_page)

