import os
from dotenv import load_dotenv
import axiom_py

load_dotenv()

client = axiom_py.Client(os.getenv("AXIOM_API_KEY"))

def log_event(message: str, dataset: str = "dsa-ingestion-worker", **fields):
    """
    Send a log event to Axiom.
    :param message: Log message string
    :param dataset: Axiom dataset name (default = dsa-ingestion-worker)
    :param fields: Additional key/value fields for structured logs
    """
    event = {"message": message, "environment": os.getenv("ENVIRONMENT"), **fields}
    try:
        client.ingest_events(dataset, [event])
    except Exception as e:
        print(f"[WARN] Failed to log to Axiom: {e}")