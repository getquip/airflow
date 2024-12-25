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

# Postman Collection: https://quipdataeng.postman.co/workspace/quip_data_eng~9066eadd-c088-4794-8fc6-2774ed80218c/collection/39993065-1e67e281-3311-4b9e-b207-d5154c7339cf?action=share&creator=39993065
with open("dags/clients/recharge/endpoint_kwargs.json", "r") as file:
	ENDPOINT_KWARGS = json.load(file)


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

    # Define tasks and run them all in parallel
    tasks = []
    for endpoint in ENDPOINT_KWARGS.keys():
        task = PythonOperator(
            task_id=f"ingesting_data_from_{endpoint}_endpoint", 
            python_callable=airbud.ingest_data,
            op_kwargs={
                "ingestion_metadata": INGESTION_METADATA,
                "endpoint": endpoint,  
                "endpoint_kwargs": ENDPOINT_KWARGS.get(endpoint),
                "paginate": True,   
                "pagination_function": paginate_responses
            },
            dag=dag
        )