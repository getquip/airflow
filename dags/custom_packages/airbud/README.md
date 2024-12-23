# Airbud
This custom package was developed for reusability of code to ingest data via API endpoint and land data to GCS and BigQuery.

## DAG Operators
The `dag_operators.py` file contains the main functions that should be used within the Airflow DAGs. The functions should be used with the `PythonOperator()` in order to create an Airflow task.


## Data Source Specific
Though Airbud is a set of python functions that can be reused across different ingestion pipelines, each API has some unique functionality. These variable functionalities are left up to the developer to create in the `airflow/dags/clients/` folder, within the respective resource folder. 

Example: `airflow/dags/clients/recharge/`

### Pagination
Every API is different in regards to how the pagination logic works. It is important to define the **pagination function** within the `clients` folder and pass the function to airbud's `ingest_data` function. The default state is no pagination.

The `ingest_data` function will pass the following fields to the custom pagination function:
- endpoint
- url
- headers
- parameters (either params, data, or json_data)
- `**kwargs` 