import os
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from uuid import uuid4
from tqdm import tqdm
import snowflake.connector

from utils.temp_utils import create_temp_dir, delete_file
from utils.logger import log_event
from land import downloader, unzipper, explorer
from utils.pd_utils import df_from_parquet

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "INGEST_DSA_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DIGITAL_SERVICES_ACT")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")


def ingest_date_snowflake(date: str, target_platform: str = "Google Maps"):
    """
    Ingest a given day's parquet files into a RAW staging table in Snowflake.
    """
    event_id = str(uuid4())
    extract_dir, parquet_files = _land_extract(date, event_id)

    try:
        total_rows = 0
        for f in tqdm(parquet_files, desc=f"Processing {date}"):
            rows = _stage_raw_snowflake(f, target_platform, event_id, date)
            total_rows += rows

        log_event(
            f"Staged {total_rows} raw rows for {date}",
            event_id=event_id,
            event_type="raw_stage_summary",
            rowcount=total_rows,
        )
    finally:
        delete_file(path=extract_dir, context="cleanup")


def _stage_raw_snowflake(file: str, target_platform: str, event_id: str, date: str) -> int:
    """
    Filter parquet to target platform, stage to Snowflake @raw stage.
    No type normalization is applied. Data will be loaded into RAW variant table.
    """
    base = os.path.splitext(os.path.basename(file))[0]
    platform_tag = target_platform.lower().replace(" ", "")
    label = f"{date}-{platform_tag}-{base}"

    # Load parquet
    raw_df = df_from_parquet(file)
    delete_file(path=file, context="post-read cleanup")

    df = raw_df.loc[raw_df["platform_name"] == target_platform].copy()
    if df.empty:
        tqdm.write(f"[{label}] skipped (no rows after filter)")
        return 0
    
    df["file_date"] = date

    tqdm.write(f"[{label}] stage {len(df)} rows")

    # Write to parquet (unchanged, no cleaning). We’ll load as VARIANT.
    tmp_dir = tempfile.mkdtemp(prefix=f"snowflake_raw_{event_id}_")
    pq_filename = f"sor-{label}.parquet"
    pq_file = os.path.join(tmp_dir, pq_filename)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, pq_file)
    tqdm.write(f"[{label}] wrote raw parquet → {pq_file}")

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
        # Ensure a RAW staging table exists
        cs.execute("""
            CREATE TABLE IF NOT EXISTS SOR_RAW (record VARIANT)
        """)

        # Use a file format that preserves logical types
        cs.execute("""
            CREATE OR REPLACE FILE FORMAT parquet_ff
            TYPE = PARQUET
            USE_LOGICAL_TYPE = TRUE
        """)

        cs.execute(f"PUT file://{pq_file} @%SOR_RAW AUTO_COMPRESS=TRUE")
        tqdm.write(f"[{label}] PUT complete")

        # Copy into raw table (everything goes into the variant column)
        cs.execute("""
            COPY INTO SOR_RAW
            FROM @%SOR_RAW
            FILE_FORMAT=parquet_ff
            ON_ERROR=CONTINUE;
        """)
        tqdm.write(f"[{label}] COPY INTO RAW complete")

        return len(df)
    finally:
        cs.close()
        conn.close()
        delete_file(path=pq_file, context="temp parquet cleanup")


def _land_extract(date: str, event_id: str):
    """Download, unzip, and find parquet files for a given date."""
    tmp_file = downloader.download_day(date, to_temp=True)
    extract_dir = create_temp_dir(prefix=f"dsa_extract_{date}_")
    unzipped_path = unzipper.unzip_file(tmp_file, extract_dir, event_id=event_id)
    delete_file(path=tmp_file, context="post-unzip cleanup")
    parquet_files = explorer.find_parquet_files(unzipped_path, event_id=event_id)
    return extract_dir, parquet_files