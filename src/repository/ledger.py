import uuid
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from uuid import UUID as PyUUID

from upload.model import IngestionLedger
from utils.logger import log_event


class IngestionLedgerRepository:

    @staticmethod
    def start_run(session: Session, file_date, event_id: str) -> IngestionLedger:
        """
        Create a new ingestion ledger entry at the start of a run.
        """
        ledger = IngestionLedger(
            file_date=file_date,
            event_id=event_id,
            started_at=datetime.utcnow(),
            success=False
        )

        event_id = PyUUID(str(event_id))
        session.add(ledger)
        try:
            session.commit()
            log_event(
                f"Ingestion run started for {file_date}",
                level="info",
                event_id=str(event_id),
                file_date=str(file_date),
                ledger_id=str(ledger.uuid)
            )
        except SQLAlchemyError as e:
            session.rollback()
            log_event(
                f"Failed to create ingestion ledger for {file_date}: {e}",
                level="error",
                file_date=str(file_date)
            )
            raise
        return ledger

    @staticmethod
    def mark_success(session: Session, ledger: IngestionLedger, rows_ingested: int) -> None:
        """
        Mark the ingestion ledger entry as successful.
        """
        ledger.rows_ingested = rows_ingested
        ledger.success = True
        ledger.finished_at = datetime.utcnow()
        try:
            session.commit()
            log_event(
                f"Ingestion completed successfully for {ledger.file_date}",
                level="info",
                ledger_id=str(ledger.uuid),
                rows_ingested=rows_ingested
            )
        except SQLAlchemyError as e:
            session.rollback()
            log_event(
                f"Failed to mark ingestion success: {e}",
                level="error",
                ledger_id=str(ledger.uuid)
            )
            raise

    @staticmethod
    def mark_failure(session: Session, ledger: IngestionLedger, error_message: str) -> None:
        """
        Mark the ingestion ledger entry as failed with error details.
        """
        ledger.success = False
        ledger.error_message = error_message
        ledger.finished_at = datetime.utcnow()
        try:
            session.commit()
            log_event(
                f"Ingestion failed for {ledger.file_date}",
                level="error",
                ledger_id=str(ledger.uuid),
                error=error_message
            )
        except SQLAlchemyError as e:
            session.rollback()
            log_event(
                f"Failed to mark ingestion failure: {e}",
                level="error",
                ledger_id=str(ledger.uuid)
            )
            raise

    @staticmethod
    def get_by_date(session: Session, file_date):
        """
        Retrieve ledger entries for a given file_date.
        """
        return session.query(IngestionLedger).filter(IngestionLedger.file_date == file_date).all()

    @staticmethod
    def get_latest(session: Session):
        """
        Retrieve the latest ingestion run (by started_at).
        """
        return session.query(IngestionLedger).order_by(IngestionLedger.started_at.desc()).first()