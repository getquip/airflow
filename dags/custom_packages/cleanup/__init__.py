import logging

from airflow.models import XCom
from airflow.utils.db import provide_session

# Initialize logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

@provide_session
def cleanup_xcom(context, session=None):
    dag_id = context["ti"]["dag_id"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()
    logging.info(f"{ dag_id } XComs cleaned up")