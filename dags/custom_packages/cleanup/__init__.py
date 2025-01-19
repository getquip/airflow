# Standard Library Imports
import logging

# Third-Party Imports
from airflow.models import XCom
from airflow.models.dagrun import DagRun
from airflow.utils.db import provide_session

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

@provide_session
def cleanup_xcom(context, session=None):
    """
    Deletes all XComs related to the current DAG run.
    This function is intended to be used as an `on_success_callback`.

    :param context: The Airflow context passed automatically.
    :param session: SQLAlchemy session provided by Airflow.
    """
    # Retrieve the current DAG ID and run ID from the context
    dag_run: DagRun = context['dag_run']
    dag_id = dag_run.dag_id

    # Perform the XCom cleanup
    deleted_count = session.query(XCom).filter(XCom.dag_id == dag_id).delete()
    session.commit()  # Commit the changes to the database

    log.info(f"{dag_id} XComs cleaned up, {deleted_count} records deleted.")
