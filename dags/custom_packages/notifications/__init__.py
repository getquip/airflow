import logging

from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.models import Variable

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def send_slack_alert(context):
    log.info(f"Composing Slack Alert...")
    # Access DAG context
    dag_name = context.get('dag').dag_id
    task_name = context.get('task').task_id
    log_url = context.get('task_instance').log_url
    logical_date = context.get('logical_date')

    # Compose message
    slack_msg = f"""
    :red_circle: *Data Ingestion Alert*
    ==================================================
    *DAG*: *{dag_name}*
    *Task*: {task_name}
    *Execution Date*: {logical_date}
    *Log URL*: {log_url}
    """

    # Create Notification
    notifier = send_slack_notification(
        text=slack_msg,
        channel=Variable.get("SLACK_CHANNEL", default_var="#alerts-data-dev"),
        username="Airflow",
        slack_conn_id="slack_default"
    )

    # Send the message
    notifier.notify(context)
    log.info(f"Slack Alert: {slack_msg}")
