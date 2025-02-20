# Import Standard Libraries
import os
import re
import csv
import stat
import json
import time
import logging
import pandas as pd
from zipfile import ZipFile
from typing import List, Dict

# Import Third Party Libraries
from airflow.models.dagrun import DagRun
from airflow.providers.sftp.hooks.sftp import SFTPHook

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

def load_csv_to_df(csv_file: str) -> pd.DataFrame:
    try:
        # Initialize an empty list to store chunks
        all_chunks = []
        
        # Loop through the file in chunks
        for chunk in pd.read_csv(
            csv_file, 
            chunksize=10000, 
            on_bad_lines='skip', 
            low_memory=True,
            delimiter=','):
            print(f"Processing csv chunk with {len(chunk)} rows...")
            # Append the chunk to the list
            all_chunks.append(chunk)
        
        # Concatenate all chunks into a single DataFrame
        full_df = pd.concat(all_chunks, ignore_index=True)
        print(f"Total rows in the DataFrame: {len(full_df)}")
        
        return full_df  # Return the combined DataFrame
        
    except Exception as e:
        print(f"Error reading the file: {e}")
        return []

def clean_column_names(
    df: pd.DataFrame, # DataFrame to clean
    json_file_path: str, # Name of the csv GCS file
    dag_run_date: str, # Date of the DAG run
    ) -> List[Dict]: # Returns csv as json records
    """Clean column names and convert CSV to JSON records."""
    
    if len(df) > 0:
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
            # replace any quotes
            cleaned_col = cleaned_col.replace('"', '')
            cleaned_col = cleaned_col.replace("'", "")
            # Replace special characters with underscores
            cleaned_col = re.sub(r'[^a-zA-Z0-9]', '_', cleaned_col)
            # Replace multiple underscores with a single one
            cleaned_col = re.sub(r'_{2,}', '_', cleaned_col)  
            # Remove leading/trailing underscores
            cleaned_col = cleaned_col.strip('_')  
            # Append the cleaned column name to the list
            cleaned_columns.append(cleaned_col)
            
        # Assign the cleaned column names back to the DataFrame
        df.columns = cleaned_columns
        
        # Store synced_at timestamp in the records
        df['source_synced_at'] = dag_run_date
        
        # Store file name in the records
        df['source_file_name'] = json_file_path
        
        # Convert the DataFrame to a list of dictionaries
        records = json.loads(df.to_json(orient='records', lines=False))
        log.debug(f"Converted {len(records)} rows to JSON records")
        
        return records
    else:
        return []

def list_sftp_files(
    sftp_conn_id: str,  # SFTP connection ID
    remote_path: str,  # Remote directory path
    endpoint_kwargs: dict,  # Get files to ignore
) -> List[str]:  # Returns list of file paths
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

            # Skip files that match the ignore list
            if any(ignore == item_path or ignore in item_path for ignore in ignore_files):
                log.info(f"Skipping {item_path}")
                continue

            # If it's a directory, recurse into it
            if stat.S_ISDIR(item.st_mode):
                get_files_recursively(sftp, item_path, files)
            else:
                files.append(item_path)

    # List files in the remote directory
    files = []
    with sftp_hook.get_conn() as sftp:
        get_files_recursively(sftp, remote_path, files)

    # Sort the files list before returning
    return sorted(files)


def download_sftp_file(
    sftp_conn_id: str, # SFTP connection ID
    source_file: str # file path
    ) -> str: # Returns list of downloaded file names (without original path)
    """Download files from a remote SFTP server."""

    # Connect to SFTP server
    sftp_hook = SFTPHook(sftp_conn_id)

    with sftp_hook.get_conn() as sftp:
        try:
            # Extract the file name for env path
            filename = os.path.basename(source_file)
            # Download the file to the current working directory without the source path
            sftp.get(source_file, filename)
            print(f"Downloaded: {filename}")
            return filename
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found on SFTP server: {filename}")
        except Exception as e:
            raise Exception(f"Error downloading {filename}: {e}")

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
    filename = os.path.basename(source_file)
    destination_path = os.path.join(processed_path, filename)

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

def move_files_on_s3(
    s3_hook: object,
    bucket_name: str, # Name of the S3 bucket
    source_file: str, # Path to the source file
    destination_file: str, # Path to the destination file
    ):
    # Step 1: Copy the file
    print(f"Moving {source_file} to {destination_file}...")
    s3_hook.copy_object(
        source_bucket_name=bucket_name,
        source_bucket_key=source_file,
        dest_bucket_name=bucket_name,
        dest_bucket_key=destination_file,
    )

    # Check that the file was copied successfully
    if s3_hook.check_for_key(destination_file, bucket_name=bucket_name):
        # Step 2: Delete the original file
        s3_hook.delete_objects(bucket=bucket_name, keys=[source_file])
        log.info(f"Successfully moved {source_file} to {destination_file}.")
    else:
        log.warn(f"Failed to copy {source_file} to {destination_file}.")

def unzip_files(zip_file_path):
    try:
        extracted_files = []
        
        # Open the ZIP file
        with ZipFile(zip_file_path) as zf:
            # List all files in the ZIP
            filenames = zf.namelist()
            
            if not filenames:
                raise ValueError("The ZIP file is empty.")
            
            # Find all CSV files in the ZIP
            csv_files = [f for f in filenames if f.endswith('.csv')]
            
            if not csv_files:
                raise ValueError("No CSV files found in the ZIP.")
            
            # Extract and save each CSV file
            for file in csv_files:
                with zf.open(file) as csv_file:
                    # Create a local path for the file
                    csv_path = os.path.basename(file)
                    
                    # Write the file to the current directory
                    with open(csv_path, 'wb') as output_file:
                        output_file.write(csv_file.read())
                    
                    # Log and store the saved file path
                    log.info(f"CSV saved to {csv_path}")
                    extracted_files.append(csv_path)
        
        return extracted_files
    
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{zip_file_path}' does not exist.")
    except ValueError as ve:
        raise ve
    except Exception as e:
        raise Exception(f"An error occurred: {e}")
