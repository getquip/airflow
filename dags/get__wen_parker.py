
# Standart Library imports
import json
from datetime import datetime

# Third-party imports
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

# Custom package imports
from custom_packages import airbud
from clients.wen_parker import GetWenParker
from custom_packages.cleanup import cleanup_xcom
from custom_packages.notifications import send_slack_alert

# Define constants for data source
PROJECT_ID = Variable.get("PROJECT_ID", default_var="quip-dw-raw-dev")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="quip_airflow_dev")
SFTP_CONN_ID = "wen_parker"
CLIENT = GetWenParker(PROJECT_ID, GCS_BUCKET, SFTP_CONN_ID)

# Define default arguments for the DAG
default_args = {
    "owner": "ammie",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "max_active_runs": 1,
}

# Define the DAG
with DAG(
    dag_id="get__wen_parker",
    default_args=default_args,
    description="A DAG to sync files from Wen Parker's SFTP server",
    schedule_interval="0 12 * * *", # Daily @ 7:00 AM EST
    start_date=datetime(2025, 1, 1),
	catchup=False,
    on_success_callback=cleanup_xcom,
    on_failure_callback=[send_slack_alert],
) as dag:

	for endpoint, endpoint_kwargs in CLIENT.endpoints.items():

		# Group tasks by endpoint
		with TaskGroup(group_id=f"get__{endpoint}") as endpoint_group:
			
			ingestion_task = PythonOperator(
				task_id=f"ingest_{endpoint}_files",
				python_callable=CLIENT.get_files,
				op_kwargs={
					"endpoint": endpoint,
					"endpoint_kwargs": endpoint_kwargs,
				},
				dag=dag,
			)

			load_to_bq_task = PythonOperator(
				task_id=f"load_{endpoint}_files_to_bq",
				python_callable=CLIENT.load_to_bq,
				op_kwargs={
					"endpoint": endpoint,
					"endpoint_kwargs": endpoint_kwargs,
				},
				dag=dag,
			)

			move_to_processed_task = PythonOperator(
				task_id=f"move_{endpoint}_files_to_processed",
				python_callable=CLIENT.move_to_processed,
				op_kwargs={"endpoint": endpoint},
				dag=dag,
   				trigger_rule="all_success", # Only run if all tasks are successful
			)

			ingestion_task >> load_to_bq_task >> move_to_processed_task