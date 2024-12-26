# Standard library imports
from datetime import datetime
import json

# Third-party imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Local package imports
from custom_packages import airbud
from clients.recharge.paginate import paginate_responses


# Define constants for data source
PROJECT_ID = Variable.get("PROJECT_ID", default_var="quip-dw-raw-dev")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="quip_airflow_dev")
HOST_GCS_BUCKET = Variable.get("HOST_GCS_BUCKET")
DATA_SOURCE_NAME = "recharge"
BASE_URL = "https://api.rechargeapps.com/"

# Update headers with API key
API_KEY = airbud.get_secrets(PROJECT_ID, DATA_SOURCE_NAME, SECRET_PREFIX="api__")
HEADERS = {
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
    download_client_files = PythonOperator(
        task_id="download_client_files",
        python_callable=airbud.download_client_files,
        op_kwargs={
            "bucket_name": HOST_GCS_BUCKET,
            "prefix": DATA_SOURCE_NAME
        },
        dag=dag,
    )

    # Define tasks and run them all in parallel
    def ingest_data_from_endpoint(**kwargs):
        # Load endpoint_kwargs from the downloaded file
        with open("/tmp/endpoint_kwargs.json", 'r') as file:
            ENDPOINTS = json.load(file)

        tasks = []
        for endpoint, endpoint_kwargs in ENDPOINTS.items():
            endpoint_kwargs["headers"] = HEADERS # Add headers
            task = PythonOperator(
                task_id=f"ingesting_data_from_{endpoint}_endpoint", 
                python_callable=airbud.ingest_data,
                op_kwargs={
                    "project_id": PROJECT_ID,
                    "dataset_name": DATA_SOURCE_NAME,
                    "gcs_bucket": GCS_BUCKET,
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

