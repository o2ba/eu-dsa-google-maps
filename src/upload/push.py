from sqlalchemy.orm import Session
from upload.model import StatementOfReasons
from utils.logger import log_event
from tqdm import tqdm
from sqlalchemy.orm import Session

def push_df_to_db(
    df,
    session: Session,
    batch_size: int,
    file: str,
    event_id: str,
):
    """
    Push a normalized DataFrame into the DB in batches with a progress bar.
    """
    rows = df.to_dict(orient="records")
    total = len(rows)

    if total == 0:
        return

    for i in tqdm(
        range(0, total, batch_size),
        desc=f"Ingesting {file}",
        unit="batch",
        total=(total + batch_size - 1) // batch_size,  
    ):
        batch = rows[i : i + batch_size]
        try:
            session.bulk_insert_mappings(StatementOfReasons, batch)
            session.commit()

            log_event(
                f"Inserted batch {i // batch_size + 1} for {file}",
                rows=len(batch),
                total=total,
                file=file,
                event_id=event_id,
                level="info",
            )
        except Exception as e:
            session.rollback()
            log_event(
                f"Failed inserting batch {i // batch_size + 1} for {file}: {e}",
                file=file,
                event_id=event_id,
                level="error",
                error=str(e),
            )
            raise