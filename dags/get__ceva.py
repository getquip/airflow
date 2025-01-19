
# Standart Library imports
import json
from datetime import datetime

# Third-party imports
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# Custom package imports
from custom_packages.cleanup import cleanup_xcom
from custom_packages.notifications import send_slack_alert
from custom_packages import airbud

# Define constants for data source
PROJECT_ID = Variable.get("PROJECT_ID", default_var="quip-dw-raw-dev")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="quip_airflow_dev")
AWS_CONN_ID = "aws_s3"

# Function to list objects in an S3 bucket
def list_s3_objects():
    """
    List objects in an S3 bucket.

    :param bucket_name: Name of the S3 bucket.
    :param aws_conn_id: Airflow connection ID for AWS (default: aws_default).
    """
    bucket = "c5aa903e-2d4b-4853-b78c-af44763ec434"
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    objects = s3_hook.list_keys(bucket_name=bucket)
    print(f"Objects in bucket {bucket}: {objects}")
    return objects


# Define default arguments for the DAG
default_args = {
    "owner": "ammie",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "max_active_runs": 1,
    "on_success_callback": cleanup_xcom,
    "on_failure_callback": [send_slack_alert],
}

# Define the DAG
with DAG(
    dag_id="get__ceva",
    default_args=default_args,
    description="A DAG to sync files from Wen Parker's SFTP server",
    schedule_interval="0 12 * * *", # Daily @ 7:00 AM EST
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

	test_connection_task = PythonOperator(
        task_id="test_aws_connection_task",
        python_callable=list_s3_objects,
        provide_context=True,
    )