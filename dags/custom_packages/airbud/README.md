# Airbud
Airbud is a custom package developed for the reusability of code to ingest data via API endpoints and land data to Google Cloud Storage (GCS) and BigQuery.

## DAG Operators
The `dag_operators` folder contains the main Python callable functions for the Airflow DAGs. These functions are designed to be used with the `PythonOperator()` in Airflow to create tasks.

## Key Classes

### `GetClient` Class
A core class in Airbud is the `GetClient` class. This class is used for specific clients (data sources). Each data source should create a child class that inherits from `GetClient` and contains source-specific attributes and methods. These must include:
- **paginate_responses**: Custom logic for handling pagination in API responses.
- **Endpoint kwargs**: Any specific parameters or configurations needed for the data source endpoint.

For example, each data source may have its own class under the `airflow/dags/clients/` folder, such as `airflow/dags/clients/recharge/`.
