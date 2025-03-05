# Standard library imports
import pandas as pd
# Third-party imports
from google.cloud import storage, bigquery
# Local package imports
from custom_packages import airbud

dataset = "alloy"
gcs_client = storage.Client(project = 'quip-dw-raw')
bq_client = bigquery.Client(project = 'quip-dw-raw')
endpoint = 'daily_sell_through_report'
source_bucket_name = 'alloy_exports_v161'
destination_bucket_name = 'quip_airflow'
gcs_bucket = gcs_client.get_bucket(destination_bucket_name)

# Get list of unprocessed csv files
alloy_bucket = gcs_client.get_bucket(source_bucket_name)
new_files = airbud.list_all_blobs(alloy_bucket, endpoint)

# Get file name
source_blob = new_files[0]
source_file_name = source_blob.name.split("/")[-1]
if source_file_name == '':
	continue

# Download file
source_blob.download_to_filename('tmp/' + source_file_name)

# Convert to df 
df = airbud.load_csv_to_df('tmp/' + source_file_name)

# Generate the destination blob name
filename_no_file_type = source_file_name.split(".")[0]

dag_run_date = str(pd.to_datetime('2025-02-25'))
json_filename = f'get/{dataset}/{endpoint}/DAG_RUN:{dag_run_date}/{filename_no_file_type}.json'

# Clean the column names and convert to JSON
records = airbud.clean_column_names(df, json_filename, dag_run_date)

# Upload the JSON data to GCS
airbud.upload_json_to_gcs(gcs_bucket, json_filename, records)

#
# 2. Insert the records to BigQuery
if len(records) > 0:
	try:
		table_ref = bq_client.dataset(dataset).table(endpoint)
		
		airbud.insert_records_to_bq(bq_client, table_ref, records)
		log.info(f"Successfully uploaded {source_file_name} to BigQuery.")
		destination_blob_name = f"{endpoint}/processed/{source_file_name}"
		airbud.move_file_in_gcs(
                            gcs_client, 
                            alloy_bucket, 
                            source_blob.name, 
                            alloy_bucket, 
                            destination_blob_name)
	except Exception as e:
		log.error(f"Error uploading {source_file_name} to BigQuery: {e}")
		bad_files.append(source_file_name)


df = pd.DataFrame(records)
df.dtypes
df.Day.min()
df.Day.max()