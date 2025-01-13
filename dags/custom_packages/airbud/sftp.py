import pandas
from typing import List, Dict

from airflow.providers.sftp.hooks.sftp import SFTPHook


def clean_column_names(csv_file, **kwargs):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
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
        
        cleaned_columns.append(cleaned_col)
        
    # Assign the cleaned column names back to the DataFrame
    df.columns = cleaned_columns

    # Store synced_at timestamp in the records
    dag_run: DagRun = kwargs.get('dag_run')
    df['source_synced_at'] = str(dag_run.execution_date)
    records = json.loads(df.to_json(orient='records', lines=False))
    
    return records

def list_sftp_files(sftp_hook, remote_path) -> List[str]:
    """
    Lists and sorts all files in a directory on an SFTP server, excluding the "processed" subfolder.

    Args:
        sftp_hook (SFTPHook): The Airflow SFTP hook instance.
        remote_path (str): The remote directory path.

    Returns:
        list: A sorted list of filenames in the specified directory, excluding files in the "processed" subfolder.
    """
    # Connect to SFTP server
    with sftp_hook.get_conn() as sftp:
        files = []
        for item in sftp.listdir_attr(remote_path):
            # Exclude the "processed" folder
            if "processed" in item.filename:
                continue
            # Append valid filenames
            files.append(item.filename)

    # Sort the files list
    return sorted(files)


def download_sftp_files(sftp_hook, remote_path, files):
    # Connect to SFTP server
    with sftp_hook.get_conn() as sftp:
        for filename in files:
            file_path = f"{remote_path}/{filename}"
            try:
                # Download the file
                sftp.get(file_path)
                print(f"Downloaded: {filename}")
            except FileNotFoundError:
                print(f"File not found on SFTP server: {filename}")
            except Exception as e:
                print(f"Error downloading {filename}: {e}")

def move_file_on_sftp(sftp_hook: SFTPHook, source_path: str, file: str) -> None:
    """
    Moves (renames) a file on the SFTP server.

    Args:
        sftp_hook (SFTPHook): The Airflow SFTP hook instance.
        source_path (str): The source file path on the SFTP server.
        destination_path (str): The destination file path on the SFTP server.
    """
    original_path = source_path + file
    destination_path = source_path + "processed/" + file
    try:
        # Connect to the SFTP server
        with sftp_hook.get_conn() as sftp:
            # Move (rename) the file
            sftp.rename(original_path, destination_path)
            log.info(f"Successfully moved file from {source_path} to processed/.")
    except Exception as e:
        log.error(f"Failed to move file from {source_path} to processed/: {e}")
