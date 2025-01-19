# Standard imports
import logging

# Third-party imports
from airflow.models import Variable

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# create next run variable if upload to bigquery is successful
def store_next_page_across_dags(dataset: str, endpoint: str, last_page: str) -> None:
    """Store the last page in an Airflow Variable."""
    bookmark_name = f"{dataset}__{endpoint}"
    Variable.set(bookmark_name, last_page)
    log.debug(f"Last page saved as Airflow Variable named: {bookmark_name}.")

def get_last_page_from_last_dag_run(name: str) -> str:
    """Retrieve the last page from an Airflow Variable."""
    try:
        next_page = Variable.get(name)
        log.debug(f"Retrieved next page: {next_page}")
    except:
        log.debug("No next page stored. Starting pagination from the beginning.")
        next_page = None
    return(next_page)
