
# Standart Library imports
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
from clients.wen_parker import GetWenParker

# Define constants for data source
PROJECT_ID = Variable.get("PROJECT_ID", default_var="quip-dw-raw-dev")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="quip_airflow_dev")
SFTP_CONN_ID = "wen_parker"
CLIENT = GetWenParker()

# Define default arguments for the DAG
default_args = {
    "owner": "ammie",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "catchup": False,
    "max_active_runs": 1,
    "on_success_callback": cleanup_xcom,
    "on_failure_callback": [send_slack_alert],
}

# Define the DAG
with DAG(
    dag_id="get__wen_parker",
    default_args=default_args,
    description="A DAG to sync files from Wen Parker's SFTP server",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2025, 1, 1)
) as dag:

	# Define ingestion tasks
	for endpoint, endpoint_kwargs in CLIENT.endpoints.items():
			ingestion_task = PythonOperator(
				task_id=f"ingest_{endpoint}_data",
				python_callable=airbud.ingest_from_sftp,
				op_kwargs={
					"project_id": PROJECT_ID,
					"bucket_name": GCS_BUCKET,
					"sftp_conn_id": SFTP_CONN_ID,
					"client": CLIENT,
					"endpoint": endpoint,
					"endpoint_kwargs": endpoint_kwargs,
				},
				dag=dag
			)