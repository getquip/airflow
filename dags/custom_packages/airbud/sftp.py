# Import Standard Libraries
import os
import stat
import json
import time
import logging
import pandas as pd
from typing import List, Dict

# Import Third Party Libraries
from airflow.models.dagrun import DagRun
from airflow.providers.sftp.hooks.sftp import SFTPHook

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

def clean_column_names(
    csv_file: str, # Path to the CSV file
    **kwargs
    ) -> List[Dict]: # Returns csv as json records
    """Clean column names and convert CSV to JSON records."""
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    log.info(f"Read {len(df)} rows from {csv_file}")

    # Get the original column names from the first row
    cleaned_columns = []
    
    for col in df.columns:
        # Convert to lowercase
        cleaned_col = col.lower()
        # Replace spaces with underscores
        cleaned_col = cleaned_col.replace(" ", "_")
        # Replace '#' with 'number'
        cleaned_col = cleaned_col.replace("#", "number")
        # Replace '%' with 'percent'
        cleaned_col = cleaned_col.replace("%", "percent")
        # Append the cleaned column name to the list
        cleaned_columns.append(cleaned_col)
        
    # Assign the cleaned column names back to the DataFrame
    df.columns = cleaned_columns

    # Store synced_at timestamp in the records
    dag_run: DagRun = kwargs.get('dag_run')
    df['source_synced_at'] = str(dag_run.execution_date)

    # Store file name in the records
    df['source_file_name'] = csv_file

    # Convert the DataFrame to a list of dictionaries
    records = json.loads(df.to_json(orient='records', lines=False))
    log.info(f"Converted {len(records)} rows to JSON records")
    return records

def list_sftp_files(
    sftp_conn_id: str,  # SFTP connection ID
    remote_path: str,  # Remote directory path
    endpoint_kwargs: dict,  # Get files to ignore
    ) -> List[str]:  # Returns list of file paths
    """List unprocessed files in a remote SFTP directory."""
    # Get files to ignore
    ignore_files = endpoint_kwargs.get("ignore", [])

    # Connect to SFTP server
    sftp_hook = SFTPHook(sftp_conn_id)

    # Helper function to recursively get files from subdirectories
    def get_files_recursively(sftp, path, files):
        for item in sftp.listdir_attr(path):
            item_path = os.path.join(path, item.filename)
            
            # Skip the "processed" folder directly under remote_path
            if item.filename == "processed" and path == remote_path:
                continue

            if any(ignore == item_path or ignore in item_path for ignore in ignore_files):
                log.info(f"Skipping {item_path}")
                continue

            # If it's a directory, recurse into it
            if stat.S_ISDIR(item.st_mode):
                get_files_recursively(sftp, item_path, files)
            else:
                files.append(item_path)

    # Assuming the function has logic to connect to the SFTP server and list files
    files = []
    with sftp_hook.get_conn() as sftp:
        get_files_recursively(sftp, remote_path, files)
    
    return files


    # Connect to SFTP server
    sftp_hook = SFTPHook(sftp_conn_id)
    with sftp_hook.get_conn() as sftp:
        files = []
        get_files_recursively(sftp, remote_path, files)

    # Sort the files list before returning
    return sorted(files)


def download_sftp_files(
    sftp_conn_id: str, # SFTP connection ID
    files: List[str] # List of file paths
    ) -> None: # Returns list of downloaded file names (without original path)
    """Download files from a remote SFTP server."""

    # Connect to SFTP server
    sftp_hook = SFTPHook(sftp_conn_id)

    with sftp_hook.get_conn() as sftp:
        for source_file in files:
            try:
                # Extract the file name for env path
                file_name = os.path.basename(source_file)
                # Download the file to the current working directory without the source path
                sftp.get(source_file, file_name)
                print(f"Downloaded: {file_name}")
            except FileNotFoundError:
                raise FileNotFoundError(f"File not found on SFTP server: {file_name}")
            except Exception as e:
                raise Exception(f"Error downloading {file_name}: {e}")

def move_file_on_sftp(
    sftp_conn_id: str,  # SFTP connection ID
    source_file: str,  # Path to the file in sftp
    processed_path: str,  # Name of the file
    ) -> None:
    """Move a file from the source path to the 'processed' directory on SFTP."""
    max_retries = 3  # Number of retry attempts
    retry_delay = 5  # Delay (in seconds) between retries
    # Connect to SFTP server
    sftp_hook = SFTPHook(sftp_conn_id)

    # Set the destination directory and path
    file_name = os.path.basename(source_file)
    destination_path = os.path.join(processed_path, file_name)

    for attempt in range(1, max_retries + 1):
        try:
            with sftp_hook.get_conn() as sftp:
                try:
                    sftp.stat(processed_path)  # Check if "processed" exists
                except FileNotFoundError:
                    log.info(f"Creating 'processed' directory {processed_path}")
                    sftp.mkdir(processed_path)

                # Move (rename) the file to the 'processed' directory
                sftp.rename(source_file, destination_path)
                log.info(f"Successfully moved file from {source_file} to {destination_path}.")
                break
        except Exception as e:
            log.error(f"Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                log.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                log.error(f"All {max_retries} attempts failed.")
                raise Exception(f"Failed to move file from {source_file} to 'processed': {e}")

