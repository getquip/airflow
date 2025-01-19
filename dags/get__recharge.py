# Standard library imports
import json
from datetime import datetime

# Third-party imports
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

# Custom package imports
from clients.recharge import GetRecharge
from custom_packages.cleanup import cleanup_xcom
from custom_packages.notifications import send_slack_alert


# Define constants for data source
PROJECT_ID = Variable.get("PROJECT_ID", default_var="quip-dw-raw-dev")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="quip_airflow_dev")

# Initialize the GetRecharge class
CLIENT = GetRecharge(PROJECT_ID, GCS_BUCKET)

# Define default arguments for the DAG
default_args = {
    "owner": "ammie",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
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
    catchup=False,
) as dag:

    for endpoint, endpoint_kwargs in CLIENT.endpoints.items():

		# Group tasks by endpoint
        with TaskGroup(group_id=f"get__{endpoint}") as endpoint_group:

            ingestion_task = PythonOperator(
                task_id= f"ingest_{ endpoint }_data",
                python_callable=CLIENT.ingest_data,
                op_kwargs={
                    "endpoint": endpoint,  
                    "endpoint_kwargs": endpoint_kwargs,
                    "paginate": True
                },
                dag=dag
            )

            upload_to_bq_task = PythonOperator(
                task_id=f"upload_{ endpoint }_data_to_bq",
                python_callable=CLIENT.load_to_bq,
                op_kwargs={
                    "endpoint": endpoint,
                    "endpoint_kwargs": endpoint_kwargs
                },
                dag=dag
            )

            ingestion_task >> upload_to_bq_task