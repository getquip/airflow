import logging

from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.models import Variable

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def send_slack_alert(context):
    logging.info(f"Composing Slack Alert...")
    # Access DAG context
    dag_name = context.get('dag').dag_id
    task_name = context.get('task').task_id
    log_url = context.get('task_instance').log_url
    execution_date = context.get('execution_date')
    project_id = Variable.get("PROJECT_ID", default_var="quip-dw-raw-dev")

    # Compose message
    slack_msg = ""
    if project_id == 'quip-dw-raw-dev':
        slack_msg += "*Environment*: Development\n"

    slack_msg += f"""
    :red_circle: *Data Ingestion Alert*
    ==================================================
    *DAG*: *{dag_name}*
    *Task*: {task_name}
    *Execution Date*: {execution_date}
    *Log URL*: {log_url}
    """

    # Create Notification
    notifier = send_slack_notification(
        text=slack_msg,
        channel="#data-engineering-alerts",
        username="Airflow",
        slack_conn_id="slack_default"
    )

    # Send the message
    notifier.notify(context)
    logging.info(f"Slack Alert: {slack_msg}")
