"""
Runs the pipeline to ingest data for a single day into Snowflake.
"""
import os
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import snowflake.connector

from upload.model import StatementOfReasons
from utils.temp_utils import create_temp_dir, delete_file
from utils.logger import log_event
from land import downloader, unzipper, explorer
from transform.normalizer import normalize_df
from utils.pd_utils import df_from_parquet

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "INGEST_DSA_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DIGITAL_SERVICES_ACT")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")


def ingest_date_snowflake(date: str, target_platform: str = "Google Maps"):
    """
    Ingest a given day's parquet files into Snowflake.
    """
    event_id = str(uuid4())
    extract_dir, parquet_files = _land_extract(date, event_id)

    try:
        total_rows = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(_process_and_load_snowflake, f, target_platform, event_id, date)
                for f in parquet_files
            ]
            for f in tqdm(as_completed(futures), total=len(futures), desc=f"Processing {date}"):
                total_rows += f.result()

        log_event(
            f"Ingested {total_rows} rows for {date}",
            event_id=event_id,
            event_type="ingestion_summary",
            rowcount=total_rows,
        )
    finally:
        delete_file(path=extract_dir, context="cleanup")


def _process_and_load_snowflake(
    file: str, target_platform: str, event_id: str, date: str
) -> int:
    """
    Transform a file (filter + normalize), write to temp parquet with a descriptive name,
    and load into Snowflake via PUT + COPY INTO. Progress is streamed with tqdm.write().
    """
    base = os.path.splitext(os.path.basename(file))[0]  # e.g. "part-0001"
    platform_tag = target_platform.lower().replace(" ", "")  # e.g. "googlemaps"
    label = f"{date}-{platform_tag}-{base}"

    # Transform
    normalized_df = _transform_file(file, target_platform, event_id)
    if normalized_df is None or normalized_df.empty:
        tqdm.write(f"[{label}] skipped (no rows after filter/normalize)")
        return 0
    tqdm.write(f"[{label}] normalized {len(normalized_df)} rows")

    # Build descriptive normalized file name
    tmp_dir = tempfile.mkdtemp(prefix=f"snowflake_norm_{event_id}_")
    norm_filename = f"sor-{label}-norm.parquet"
    norm_file = os.path.join(tmp_dir, norm_filename)

    # Write normalized dataframe to parquet
    table = pa.Table.from_pandas(normalized_df, preserve_index=False)
    pq.write_table(table, norm_file)
    tqdm.write(f"[{label}] wrote parquet → {norm_file}")

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cs = conn.cursor()

    try:
        # Upload normalized parquet into STATEMENT_OF_REASONS table stage
        cs.execute(f"PUT file://{norm_file} @%STATEMENT_OF_REASONS AUTO_COMPRESS=TRUE")
        tqdm.write(f"[{label}] PUT complete")

        # Copy into table: case-insensitive column matching
        cs.execute("""
            COPY INTO STATEMENT_OF_REASONS
            FROM @%STATEMENT_OF_REASONS
            FILE_FORMAT=(TYPE=PARQUET)
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            ON_ERROR=CONTINUE;
        """)
        tqdm.write(f"[{label}] COPY INTO complete")

        log_event(
            f"Loaded {len(normalized_df)} rows into Snowflake from {norm_filename}",
            event_id=event_id,
            file=norm_filename,
            rowcount=len(normalized_df),
        )
        return len(normalized_df)

    finally:
        cs.close()
        conn.close()
        delete_file(path=file, context="post-load cleanup")
        delete_file(path=norm_file, context="temp parquet cleanup")

def _land_extract(date: str, event_id: str):
    """Download, unzip, and find parquet files for a given date."""
    tmp_file = downloader.download_day(date, to_temp=True)
    extract_dir = create_temp_dir(prefix=f"dsa_extract_{date}_")
    unzipped_path = unzipper.unzip_file(tmp_file, extract_dir, event_id=event_id)
    delete_file(path=tmp_file, context="post-unzip cleanup")
    parquet_files = explorer.find_parquet_files(unzipped_path, event_id=event_id)
    return extract_dir, parquet_files


def _transform_file(file: str, target_platform: str, event_id: str):
    """Read parquet → filter → normalize dataframe."""
    raw_df = df_from_parquet(file)
    delete_file(path=file, context="post-read cleanup")

    # Filter by platform
    raw_df = raw_df.loc[raw_df["platform_name"] == target_platform]
    if raw_df.empty:
        return None

    # Pass the model class so normalize_df can map schema
    normalized_df = normalize_df(raw_df, StatementOfReasons, file)
    if normalized_df.empty:
        log_event(
            f"No valid data after normalization: {file}",
            level="warning",
            file=file,
            event_id=event_id,
        )
        return None

    return normalized_df