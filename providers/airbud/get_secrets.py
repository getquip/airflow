from google.cloud import secretmanager
import json

def get_secrets(PROJECT_ID, BQ_DATASET_NAME,SECRETS_PREFIX, version_id="latest"):
    """
    Access a secret from Google Cloud Secret Manager.
    
    Args:
        PROJECT_ID (str): GCP project ID where the secret is stored.
        secret_id (str): The ID of the secret to access.
        version_id (str, optional): The version of the secret. Defaults to "latest".

    Returns:
        str: The secret payload as a string.
    """
    try:
        # Create the Secret Manager client
        client = secretmanager.SecretManagerServiceClient()
        
        # Build the name of the secret version
        secret_name = f"projects/{PROJECT_ID}/secrets/{SECRETS_PREFIX}{BQ_DATASET_NAME}/versions/{version_id}"
        
        # Access the secret version
        response = client.access_secret_version(name=secret_name)
        
        # Extract the secret payload
        secret_payload = response.payload.data.decode("UTF-8")
        return json.loads(secret_payload)
    
    except Exception as e:
        print(f"Error accessing secret: {e}")
        return None