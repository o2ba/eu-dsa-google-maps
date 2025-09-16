import os
import tempfile
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime
from tqdm import tqdm

BASE_URL_PREFIX = (
    "https://d3vax7phxnku8l.cloudfront.net/raw/pqt/data/"
    "tdb_data/global___full/daily_dumps_chunked/sor-global-"
)
BASE_URL_SUFFIX = "-full.parquet.zip"


def _get_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _save_response_to_file(response, file_path: str, day: str) -> None:
    total_size = int(response.headers.get("Content-Length", 0))
    chunk_size = 8192

    with open(file_path, "wb") as f, tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=f"sor-global-{day}-full.parquet.zip",
        ascii=True,
    ) as pbar:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))


def download_day(day: str, to_temp: bool = True) -> str:
    """
    Download CSA data for a given day with retries and a tqdm progress bar.
    Returns the local file path.
    """
    # Ensure valid day format
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date: {day}. Expected format YYYY-MM-DD.")

    url = f"{BASE_URL_PREFIX}{day}{BASE_URL_SUFFIX}"
    out_dir = tempfile.gettempdir() if to_temp else os.getcwd()
    filename = f"sor-global-{day}-full.parquet.zip"
    file_path = os.path.join(out_dir, filename)

    session = _get_session()

    try:
        with session.get(url, stream=True, timeout=(10, 1200)) as response:
            response.raise_for_status()
            _save_response_to_file(response, file_path, day)
        print(f"[✔] Download complete: {file_path}")
    except requests.Timeout:
        print(f"[✘] Download timeout for {url}")
        raise
    except requests.RequestException as e:
        print(f"[✘] Failed to download {url}: {e}")
        raise

    return file_path