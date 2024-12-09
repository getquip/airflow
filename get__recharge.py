from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from providers import airbud
from airflow.models import Variable

# Define constants
## All-caps variables are constants needed for every ingestion pipeline.
## Lowercase variables are specific to this pipeline.
BQ_DATASET_NAME = "recharge"
SECRET_PREFIX = "api__"
recharge_api_key = airbud.get_secrets(BQ_DATASET_NAME, SECRET_PREFIX)['api_key']
HEADERS = {
    "X-Recharge-Access-Token": recharge_api_key,
    "X-Recharge-Version": "2021-11"
}
BASE_URL = "https://api.rechargeapps.com/"
PROJECT_ID = Variable.get("project_id")
GCS_BUCKET_NAME = Variable.get("GCS_OUTPUT_BUCKET_NAME")
# Define pagination constants
pagination_args = {
    "pagination_key": "next_cursor",
    "pagination_query":"page_info"
}

# Postman Collection: https://quipdataeng.postman.co/workspace/quip_data_eng~9066eadd-c088-4794-8fc6-2774ed80218c/collection/39993065-1e67e281-3311-4b9e-b207-d5154c7339cf?action=share&creator=39993065
endpoint_kwargs = {
    "events": {
        "jsonl_path": "events",
        "params": {"limit": 250},
        "destination_blob_name": {
            "dag_run_date": "{{ ds }}",
            "date_range": "created_at"
        }
    },
    "credit_accounts": {
        "jsonl_path": "credit_accounts",
        "params": {"limit": 250},
        "destination_blob_name": {
            "dag_run_date": "{{ ds }}",
            "date_range": "updated_at"
        }
    },
    "credit_adjustments": {
        "jsonl_path": "credit_adjustments",
        "params": {"limit": 250},
        "destination_blob_name": {
            "dag_run_date": "{{ ds }}",
            "date_range": "updated_at"
        }
    },
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

# Define tasks

    task = PythonOperator(
        task_id=f"process_events_data",
        python_callable=airbud.ingest_data,
        op_kwargs={
            "endpoint": "events",
            "project_id": PROJECT_ID,
            "bucket_name": GCS_BUCKET_NAME,
            "dataset_name": BQ_DATASET_NAME,
            "params": endpoint_kwargs.get("events")["params"],
            "jsonl_path": endpoint_kwargs.get("events")["jsonl_path"],
            "paginations_args": paginations_args,
            "destination_blob_name": endpoint_kwargs.get("events")["destination_blob_name"],
        },
        dag=dag,
    )
