import os
import tempfile
import requests
from requests.adapters import HTTPAdapter, Retry
from utils.logger import log_event
from datetime import datetime


BASE_URL_PREFIX = "https://d3vax7phxnku8l.cloudfront.net/raw/pqt/data/tdb_data/global___full/daily_dumps_chunked/sor-global-"
BASE_URL_SUFFIX = "-full.parquet.zip"


def _get_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _save_response_to_file(response, file_path: str, day: str, event_id: str) -> None:
    total_size = int(response.headers.get("Content-Length", 0))
    log_event(
        f"Starting download for sor-global-{day}-full.parquet.zip",
        total_size_mb=total_size / (1024 * 1024),
        event_id=event_id,
        event_type="download_start",
        download_file_date=day,
    )
    bytes_downloaded = 0
    last_logged_percent = 0

    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if not chunk:
                continue
            f.write(chunk)
            bytes_downloaded += len(chunk)

            if total_size > 0:
                percent = int(bytes_downloaded * 100 / total_size)
                if percent >= last_logged_percent + 10:
                    log_event(
                        f"Download {percent}% complete",
                        percent=percent,
                        download_file_date=day,
                        event_id=event_id,
                        event_type="download_progress",
                        progress_mb=bytes_downloaded / (1024 * 1024),
                    )
                    last_logged_percent = percent

    log_event(
        f"Download Complete for sor-global-{day}-full.parquet.zip",
        file=file_path,
        size_mb=bytes_downloaded / (1024 * 1024),
        event_id=event_id,
        event_type="download_complete",
        download_file_date=day,
    )


def download_day(day: str, event_id: str, to_temp: bool = True) -> str:
    """
    Downloads CSA data for a given day with retries, timeout, and progress logging.
    """
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        log_event(
            f"Invalid date format provided: {day}",
            level="error",
            download_file_date=day,
            event_id=event_id,
        )
        raise ValueError(f"Invalid date: {day}. Expected format YYYY-MM-DD.")

    url = f"{BASE_URL_PREFIX}{day}{BASE_URL_SUFFIX}"
    out_dir = tempfile.gettempdir() if to_temp else os.getcwd()
    filename = f"sor-global-{day}-full.parquet.zip"
    file_path = os.path.join(out_dir, filename)

    session = _get_session()

    try:
        with session.get(url, stream=True, timeout=(10, 1200)) as response:
            response.raise_for_status()
            _save_response_to_file(response, file_path, day, event_id)
    except requests.Timeout:
        log_event(
            f"Download timeout for {url}",
            level="error",
            download_file_date=day,
            event_id=event_id,
            error="Read timed out",
        )
        raise
    except requests.RequestException as e:
        log_event(
            f"Failed to download {url}",
            level="error",
            download_file_date=day,
            event_id=event_id,
            error=str(e),
        )
        raise

    return file_path