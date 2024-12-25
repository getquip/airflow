# Standard library imports
from datetime import datetime
import json

# Third-party imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDownloadFileOperator
from airflow.models import Variable

# Local package imports
from custom_packages import airbud
from clients.recharge.paginate import paginate_responses


# Define constants for data source
PROJECT_ID = Variable.get("PROJECT_ID", default_var="quip-dw-raw-dev")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="quip_airflow_dev")
DATA_SOURCE_NAME = "recharge"
BASE_URL = "https://api.rechargeapps.com/"
INGESTION_METADATA = {
    "project_id": PROJECT_ID,
    "dataset_name": DATA_SOURCE_NAME,
    "base_url": BASE_URL,
    "gcs_bucket_name": GCS_BUCKET,
}

# Update headers with API key
SECRET_PREFIX = "api__"
API_KEY = airbud.get_secrets(PROJECT_ID, DATA_SOURCE_NAME, SECRET_PREFIX)
INGESTION_METADATA["headers"] = {
    "X-Recharge-Access-Token": API_KEY['api_key'],
    "X-Recharge-Version": "2021-11"
}


# Define the DAG
default_args = {
    "owner": "ammie",
    "depends_on_past": False, 
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id = "get__recharge",
    default_args=default_args,
    description="A DAG to fetch Recharge data and load into GCS and BigQuery",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    
    # Task to download the 'endpoint_kwargs.json' file from GCS to local filesystem
    # Postman Collection: https://quipdataeng.postman.co/workspace/quip_data_eng~9066eadd-c088-4794-8fc6-2774ed80218c/collection/39993065-1e67e281-3311-4b9e-b207-d5154c7339cf?action=share&creator=39993065
    download_endpoint_kwargs = GCSDownloadFileOperator(
        task_id="download_endpoint_kwargs",
        bucket_name=GCS_BUCKET,
        object_name="clients/{DATA_SOURCE_NAME}/endpoint_kwargs.json",  # GCS file path
        local_file="/tmp/endpoint_kwargs.json",  # Local path to save the file
        dag=dag,
    )

    # Define tasks and run them all in parallel
    def ingest_data_from_endpoint(**kwargs):
        # Load endpoint_kwargs from the downloaded file
        with open("/tmp/endpoint_kwargs.json", 'r') as file:
            ENDPOINT_KWARGS = json.load(file)

        tasks = []
        for endpoint, endpoint_kwargs in ENDPOINT_KWARGS.items():
            task = PythonOperator(
                task_id=f"ingesting_data_from_{endpoint}_endpoint", 
                python_callable=airbud.ingest_data,
                op_kwargs={
                    "ingestion_metadata": INGESTION_METADATA,
                    "endpoint": endpoint,  
                    "endpoint_kwargs": endpoint_kwargs,
                    "paginate": True,   
                    "pagination_function": paginate_responses
                },
                dag=dag
            )
            tasks.append(task)
        return tasks
    
    # Create ingestion tasks from each endpoint (in parallel)
    create_ingest_tasks = PythonOperator(
        task_id='create_ingest_tasks',
        python_callable=ingest_data_from_endpoint,
        dag=dag
    )

    # Set task dependencies
    download_endpoint_kwargs >> create_ingest_tasks  # First download, then create ingestion tasks