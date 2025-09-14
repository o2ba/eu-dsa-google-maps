"""
Runs the pipeline to ingest data for a single day.
"""
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from uuid import uuid4

from utils.temp_utils import create_temp_dir, delete_file
from utils.logger import log_event

from land import downloader, unzipper, explorer
from transform.normalizer import normalize_df
from utils.pd_utils import df_from_parquet
from upload.model import StatementOfReasons
from repository.ledger import IngestionLedgerRepository

from tqdm import tqdm


DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def ingest_date(date: str, target_platform: str = "Google Maps"):
    event_id = str(uuid4())
    total_rows_ingested = 0
    extract_dir, parquet_files = _land_extract(date, event_id)

    with SessionLocal() as session:
        ledger = IngestionLedgerRepository.start_run(
            session, file_date=date, event_id=event_id
        )

        try:
            for file in tqdm(parquet_files, desc=f"Processing {date}", unit="file"):
                normalized_df = _transform_file(file, target_platform, event_id)
                if normalized_df is None:
                    continue

                rows = _load_to_db(normalized_df, ledger.uuid, file, event_id)
                total_rows_ingested += rows

            IngestionLedgerRepository.mark_success(
                session, ledger, rows_ingested=total_rows_ingested
            )

        except Exception as e:
            IngestionLedgerRepository.mark_failure(
                session, ledger, error_message=str(e)
            )
            raise
        finally:
            # Cleanup on fail
            delete_file(event_id=event_id, path=extract_dir, context="post-processing cleanup")

def _land_extract(date: str, event_id: str):
    """Download, unzip, and find parquet files for a given date."""
    tmp_file = downloader.download_day(date, to_temp=True, event_id=event_id)
    extract_dir = create_temp_dir(prefix=f"dsa_extract_{date}_")
    unzipped_path = unzipper.unzip_file(tmp_file, extract_dir, event_id=event_id)
    delete_file(event_id=event_id, path=tmp_file, context="post-unzip cleanup")
    parquet_files = explorer.find_parquet_files(unzipped_path, event_id=event_id)
    return extract_dir, parquet_files


def _transform_file(file: str, target_platform: str, event_id: str):
    """Read parquet -> filter -> normalize dataframe."""
    raw_df = df_from_parquet(file)
    delete_file(event_id=event_id, path=file, context="post-read cleanup")

    # Filter
    raw_df = raw_df.loc[raw_df["platform_name"] == target_platform]
    if raw_df.empty:
        return None

    # Normalize
    normalized_df = normalize_df(raw_df, StatementOfReasons, file)
    if normalized_df.empty:
        log_event(
            f"No valid data found after normalization: {file}",
            level="warning",
            file=file,
            event_id=event_id,
        )
        return None

    return normalized_df


def _load_to_db(df: pd.DataFrame, ingestion_id: str, file: str, event_id: str):
    """Attach ledger id and insert dataframe into DB."""
    df["ingestion_id"] = ingestion_id
    log_event(
        f"Prepared {len(df)} rows for ingestion from {file}",
        file=file,
        event_id=event_id,
        event_type="ingestion_prep",
        rowcount=len(df),
    )
    df.to_sql(
        StatementOfReasons.__tablename__,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10_000,
    )
    return len(df)