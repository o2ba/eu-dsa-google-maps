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


def delete_file(path: str, context: str = "") -> None:
    if not os.path.isfile(path):
        return
    os.remove(path)