from sqlalchemy import Column, Integer, Date, Boolean, Text, func
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship
from snowflake.sqlalchemy import VARIANT, STRING
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class StatementOfReasons(Base):
    __tablename__ = "statement_of_reasons"

    # Snowflake: STRING DEFAULT UUID_STRING()
    uuid = Column(
        STRING(36),
        primary_key=True,
        server_default=func.uuid_string()  # Use DB default, matches Snowflake DDL
    )

    # In your DDL: ingestion_id STRING
    ingestion_id = Column(STRING(36), ForeignKey("ingestion_ledger.uuid"))

    # VARIANT maps to Snowflake JSON-like storage
    decision_visibility = Column(VARIANT)
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

    content_type = Column(VARIANT)
    content_type_other = Column(Text)
    content_language = Column(Text)
    content_date = Column(Date)
    content_id_ean = Column(Text)

    territorial_scope = Column(VARIANT)
    application_date = Column(Date)
    decision_facts = Column(Text)

    source_type = Column(Text)
    source_identity = Column(Text)

    automated_detection = Column(Boolean)
    automated_decision = Column(Text)

    platform_name = Column(Text)
    platform_uid = Column(Text)

    created_at = Column(TIMESTAMP(timezone=True))
    loaded_at = Column(
        TIMESTAMP(timezone=True), server_default=func.current_timestamp()
    )

    ingestion = relationship("IngestionLedger", back_populates="statements")

def get_model_schema(model_cls):
    mapper = inspect(model_cls)
    schema = {}
    for column in mapper.columns:
        col_name = column.key
        col_type = column.type
        schema[col_name] = col_type
    return schema