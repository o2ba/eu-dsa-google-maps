import tempfile
import os
from utils.logger import log_event

def create_temp_dir(prefix: str = "temp_dir_") -> str:
    """
    Creates a temporary directory with a given prefix.
    Returns the path to the created directory.
    """
    # Create a unique temp directory under the system temp path
    temp_dir = tempfile.mkdtemp(prefix=prefix, dir=tempfile.gettempdir())
    return temp_dir


def delete_file(event_id: str, path: str, context: str = "") -> None:
    if not os.path.isfile(path):
        log_event(
            f"File not found, nothing to delete: {path}",
            level="warning",
            event_id=event_id,
            file=path,
            context=context,
        )
        return

    try:
        os.remove(path)
        log_event(
            f"Deleted file: {path}",
            level="info",
            event_id=event_id,
            event_type="file_deletion",
            file=path,
            context=context,
        )
    except Exception as e:
        log_event(
            f"Failed to delete file: {path} ({e})",
            level="error",
            file=path,
            context=context,
            error=str(e),
        )