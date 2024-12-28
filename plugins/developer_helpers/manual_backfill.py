from custom_packages import airbud
from clients.recharge import GetRecharge


# Define constants for data source
PROJECT_ID = "quip-dw-raw"
GCS_BUCKET = "quip_airflow"

# Initialize the GetRecharge class
API_KEY = airbud.get_secrets(PROJECT_ID, 'recharge', prefix="api__")
recharge = GetRecharge(API_KEY['api_key'])
