# Standard library imports
import json
from typing import Dict
from logging import getLogger

# Third-party imports
from google.cloud import secretmanager

# Initialize logger
log = getLogger(__name__)

def get_secrets(
        project_id: str, # GCP project ID where the secret is stored
        dataset_name: str, # Used to construct the secret name
        prefix: str, # Used to construct the secret name
        version_id="latest"
    ) -> str: # The secret payload
    """
    Access a secret from Google Cloud Secret Manager.
    """
    try:
        # Create the Secret Manager client
        client = secretmanager.SecretManagerServiceClient()
        
        # Build the name of the secret version
        secret_name = f"projects/{project_id}/secrets/{prefix}{dataset_name}/versions/{version_id}"
        log.info(f"Accessing secret: {secret_name}")
        # Access the secret version
        response = client.access_secret_version(name=secret_name)
        
        # Extract the secret payload
        secret_payload = response.payload.data.decode("UTF-8")
        log.info(f"Accessed secret successfully.")
        return json.loads(secret_payload)
    
    except Exception as e:
        print(f"Error accessing secret: {e}")
        return {"api_key":""}