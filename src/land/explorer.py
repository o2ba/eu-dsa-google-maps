import os
from typing import List
from utils.logger import log_event

def find_parquet_files(directory: str, event_id: str) -> List[str]:
    """
    Recursively finds all .parquet files in a directory (walker)
    :returns: List of full paths to .parquet files
    """
    parquet_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(".parquet"):
                parquet_files.append(os.path.join(root, file))

    log_event(
        f"Found {len(parquet_files)} parquet file(s) in {directory}",
        count=len(parquet_files),
        event_id=event_id,
        event_type="parquet_discovery",
        directory=directory,
    )

    return parquet_files