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
from custom_packages.notifications import send_slack_alert
from custom_packages import airbud
from clients.recharge import GetRecharge


# Define constants for data source
PROJECT_ID = Variable.get("PROJECT_ID", default_var="quip-dw-raw-dev")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="quip_airflow_dev")

# Initialize the GetRecharge class
API_KEY = airbud.get_secrets(PROJECT_ID, 'recharge', prefix="api__")
recharge = GetRecharge(API_KEY['api_key'])

# Define default arguments for the DAG
default_args = {
    "owner": "ammie",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "catchup": False,
    "max_active_runs": 1,
    "on_success_callback": cleanup_xcom,
    "on_failure_callback": [send_slack_alert],
}

with DAG(
    dag_id="get__recharge",
    description="A DAG to fetch Recharge data and load into GCS and BigQuery",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    default_args=default_args,
    start_date=datetime(2024, 12, 1),
) as dag:

    # Define ingestion tasks
    for endpoint, endpoint_kwargs in recharge.endpoints.items():
        with TaskGroup(group_id=f"get__{endpoint}") as endpoint_group:

            ingestion_task = PythonOperator(
                task_id= f"ingest_{ endpoint }_data",
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
                task_id=f"upload_{ endpoint }_data_to_bq",
                python_callable=airbud.load_data_to_bq,
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

            ingestion_task >> upload_to_bq_task