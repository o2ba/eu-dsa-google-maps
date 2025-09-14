import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from uuid import uuid4

from utils.temp_utils import create_temp_dir, delete_file
from utils.logger import log_event

from land import downloader, unzipper, explorer
from transform.normalizer import normalize_df
from utils.pd_utils import df_from_parquet
from upload.model import StatementOfReasons
from repository.ledger import IngestionLedgerRepository

from tqdm import tqdm
import typer

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def main(date: str, target_platform: str = "Google Maps"):
    event_id = str(uuid4())
    total_rows_ingested = 0

    # 🟡 LAND → download/unzip/parquet discovery
    tmp_file = downloader.download_day(date, to_temp=True, event_id=event_id)
    extract_dir = create_temp_dir(prefix=f"dsa_extract_{date}_")
    unzipped_path = unzipper.unzip_file(tmp_file, extract_dir, event_id=event_id)
    delete_file(event_id=event_id, path=tmp_file, context="post-unzip cleanup")
    parquet_files = explorer.find_parquet_files(unzipped_path, event_id=event_id)

    with SessionLocal() as session:
        ledger = IngestionLedgerRepository.start_run(
            session, file_date=date, event_id=event_id
        )

        try:
            for file in tqdm(parquet_files, desc="Processing Parquet Files", unit="file"):
                # 🟡 EXTRACT raw parquet
                raw_df = df_from_parquet(file)
                delete_file(event_id=event_id, path=file, context="post-read cleanup")

                # 🟡 FILTER by platform
                raw_df = raw_df.loc[raw_df["platform_name"] == target_platform]
                if raw_df.empty:
                    del raw_df
                    continue

                # 🟡 TRANSFORM to DB-safe dataframe
                normalized_df = normalize_df(raw_df, StatementOfReasons, file)
                del raw_df

                if normalized_df.empty:
                    log_event(
                        f"No valid data found in parquet after normalization: {file}",
                        level="warning",
                        file=file,
                        event_id=event_id,
                    )
                    del normalized_df
                    continue

                # Inject ingestion_id for linkage
                normalized_df["ingestion_id"] = ledger.uuid

                log_event(
                    f"Prepared {len(normalized_df)} rows for ingestion from {file}",
                    file=file,
                    event_id=event_id,
                    rowcount=len(normalized_df),
                )

                # 🟡 LOAD into DB
                normalized_df.to_sql(
                    StatementOfReasons.__tablename__,
                    engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10_000,  # safe chunk size
                )

                total_rows_ingested += len(normalized_df)
                del normalized_df

            # 🟢 Mark ledger success
            IngestionLedgerRepository.mark_success(
                session, ledger, rows_ingested=total_rows_ingested
            )

        except Exception as e:
            # 🔴 Mark ledger failure
            IngestionLedgerRepository.mark_failure(
                session, ledger, error_message=str(e)
            )
            raise

    # 🟡 CLEANUP
    delete_file(event_id=event_id, path=extract_dir, context="post-processing cleanup")


if __name__ == "__main__":
    typer.run(main)