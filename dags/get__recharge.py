from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from custom_packages import airbud

# Define constants
DATA_SOURCE_NAME = "recharge"
INGESTION_METADATA = {
    "project_id": "quip-dw-raw-dev", #Variable.get("project_id")
    "dataset_name": DATA_SOURCE_NAME,
    "base_url": "https://api.rechargeapps.com/",
    "gcs_bucket_name": "airflow_outputs", #Variable.get("GCS_OUTPUT_BUCKET_NAME")
}

# Update headers with API key

SECRET_PREFIX = "api__"
import logging
secrets = airbud.get_secrets("quip-dw-raw-dev", DATA_SOURCE_NAME, SECRET_PREFIX)
logging.info(f"Secrets fetched: {secrets}")
if secrets:
    API_KEY = secrets.get('api_key')
    logging.info(f"API Key: {API_KEY}")

API_KEY = airbud.get_secrets("quip-dw-raw-dev", DATA_SOURCE_NAME, SECRET_PREFIX)['api_key']
INGESTION_METADATA["headers"] = {
    "X-Recharge-Access-Token": API_KEY,
    "X-Recharge-Version": "2021-11"
}
# Pagination constants
PAGINATION_ARGS = {
    "pagination_key": "next_cursor",
    "pagination_query":"page_info"
}

# Postman Collection: https://quipdataeng.postman.co/workspace/quip_data_eng~9066eadd-c088-4794-8fc6-2774ed80218c/collection/39993065-1e67e281-3311-4b9e-b207-d5154c7339cf?action=share&creator=39993065
ENDPOINT_KWARGS = {
    "events": {
        "jsonl_path": "events",
        "params": {"limit": 250},
        "destination_blob_name": {
            "dag_run_date": "{{ ds }}",
            "date_range": "created_at"
        },
        "bigquery_metadata": {
            "partitioning_type": "DAY", # DAY, MONTH, YEAR
            "partitioning_field": "created_at",
            "clustering_fields": ["object_type", "verb", "customer_id", "id"]
        }
    },
    "credit_accounts": {
        "jsonl_path": "credit_accounts",
        "params": {"limit": 250},
        "destination_blob_name": {
            "dag_run_date": "{{ ds }}",
            "date_range": "updated_at"
        },
        "bigquery_metadata": {
            "partitioning_type": "DAY", # DAY, MONTH, YEAR
            "partitioning_field": "updated_at",
            "clustering_fields": ["name", "type", "customer_id", "id"]
        }
    },
    "credit_adjustments": {
        "jsonl_path": "credit_adjustments",
        "params": {"limit": 250},
        "destination_blob_name": {
            "dag_run_date": "{{ ds }}",
            "date_range": "updated_at"
        },
        "bigquery_metadata": {
            "partitioning_type": "DAY", # DAY, MONTH, YEAR
            "partitioning_field": "updated_at",
            "clustering_fields": ["type", "credit_account_id", "id"]
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
                "pagination_args": PAGINATION_ARGS
            },
            dag=dag
        )