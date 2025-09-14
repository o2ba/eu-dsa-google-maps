from sqlalchemy import Column, Integer, Date, Boolean, Text, func
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship
import uuid

Base = declarative_base()


class IngestionLedger(Base):
    __tablename__ = "ingestion_ledger"

    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    file_date = Column(Date, nullable=False)  
    event_id = Column(UUID(as_uuid=True), nullable=False, unique=True)

    started_at = Column(TIMESTAMP(timezone=True),
                        nullable=False,
                        server_default=func.now())
    finished_at = Column(TIMESTAMP(timezone=True))

    rows_ingested = Column(Integer, default=0)

    success = Column(Boolean, default=False, nullable=False)
    error_message = Column(Text)

    statements = relationship("StatementOfReasons", back_populates="ingestion")


class StatementOfReasons(Base):
    __tablename__ = "statement_of_reasons"

    uuid = Column(UUID(as_uuid=True), primary_key=True,
                  default=uuid.uuid4)
    
    ingestion_id = Column(UUID(as_uuid=True), ForeignKey("ingestion_ledger.uuid"))

    decision_visibility = Column(JSONB)
    decision_visibility_other = Column(Text)
    end_date_visibility_restriction = Column(Date)

    decision_monetary = Column(Text)
    decision_monetary_other = Column(Text)
    end_date_monetary_restriction = Column(Date)

    decision_provision = Column(Text)
    end_date_service_restriction = Column(Date)

    decision_account = Column(Text)
    end_date_account_restriction = Column(Date)
    account_type = Column(Text)

    decision_ground = Column(Text)
    decision_ground_reference_url = Column(Text)
    illegal_content_legal_ground = Column(Text)
    illegal_content_explanation = Column(Text)
    incompatible_content_ground = Column(Text)
    incompatible_content_explanation = Column(Text)
    incompatible_content_illegal = Column(Text)

    category = Column(Text)
    category_addition = Column(Text)
    category_specification = Column(Text)
    category_specification_other = Column(Text)

    content_type = Column(JSONB)
    content_type_other = Column(Text)
    content_language = Column(Text)
    content_date = Column(Date)
    content_id_ean = Column(Text)

    territorial_scope = Column(JSONB)
    application_date = Column(Date)
    decision_facts = Column(Text)

    source_type = Column(Text)
    source_identity = Column(Text)

    automated_detection = Column(Boolean)  # Yes/No → True/False
    automated_decision = Column(Text)

    platform_name = Column(Text)
    platform_uid = Column(Text)

    created_at = Column(TIMESTAMP)  # their created_at

    ingestion = relationship("IngestionLedger", back_populates="statements")

def get_model_schema(model_cls):
    mapper = inspect(model_cls)
    schema = {}
    for column in mapper.columns:
        col_name = column.key
        col_type = column.type
        schema[col_name] = col_type
    return schema