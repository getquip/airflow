from airflow.models import XCom
from airflow.utils.db import provide_session

@provide_session
def cleanup_xcom(context, session=None):
    dag_id = context["ti"]["dag_id"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()