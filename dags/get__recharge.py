# Standard library imports
from datetime import datetime
import json

# Third-party imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Custom package imports
from custom_packages.cleanup import cleanup_xcom
from custom_packages import airbud
from clients.recharge import GetRecharge


# Define constants for data source
PROJECT_ID = Variable.get("PROJECT_ID", default_var="quip-dw-raw-dev")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="quip_airflow_dev")

# Initialize the GetRecharge class
API_KEY = airbud.get_secrets(PROJECT_ID, 'recharge', prefix="api__")
recharge = GetRecharge(API_KEY['api_key'])

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
    on_success_callback=cleanup_xcom
) as dag:

    # Define ingestion tasks
    for endpoint, endpoint_kwargs in recharge.endpoints.items():
        with TaskGroup(group_id=f"get__{endpoint}") as endpoint_group:

            ingestion_task = PythonOperator(
                task_id= f"ingesting_{endpoint}_data",
                python_callable=airbud.ingest_data,
                op_kwargs={
                    "project_id": PROJECT_ID,
                    "bucket_name": GCS_BUCKET,
                    "client": recharge,
                    "endpoint": endpoint,  
                    "endpoint_kwargs": endpoint_kwargs,
                    "paginate": True
                },
                dag=dag
            )

            upload_to_bq_task = PythonOperator(
                task_id=f"uploading_{endpoint}_data_to_bq",
                python_callable=airbud.load_data_to_bq,
                op_kwargs={
                    "project_id": PROJECT_ID,
                    "bucket_name": GCS_BUCKET,
                    "dataset_name": recharge.dataset,
                    "endpoint": endpoint,
                    "endpoint_kwargs": endpoint_kwargs,
                    "paginate": True
                },
                dag=dag
            )

            ingestion_task >> upload_to_bq_task