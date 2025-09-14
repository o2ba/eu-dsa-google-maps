import json
import uuid
import pandas as pd
import pytz
from utils.logger import log_event
from sqlalchemy import Column, String, Date, Boolean, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP

def convert_value(value, col_type, col_name, file):
    """
    Convert a parquet value into a DB-compatible Python object
    based on SQLAlchemy column type.
    """
    try:
        if pd.isna(value):
            return None

        # UUID
        if isinstance(col_type, UUID):
            return uuid.UUID(str(value))

        # DATE
        if isinstance(col_type, Date):
            return pd.to_datetime(value, errors="coerce").date()

        # TIMESTAMPTZ (timestamp with time zone)
        if isinstance(col_type, TIMESTAMP) and col_type.timezone:
            ts = pd.to_datetime(value, errors="coerce", utc=True)
            if pd.isna(ts):
                return None
            return ts.to_pydatetime().astimezone(pytz.UTC)

        # BOOLEAN
        if isinstance(col_type, Boolean):
            if isinstance(value, str):
                return value.strip().lower() in ("1", "true", "yes")
            if isinstance(value, (int, float)):
                return bool(int(value))
            return bool(value)

        # JSONB
        if isinstance(col_type, JSONB):
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            if isinstance(value, str):
                try:
                    json.loads(value)
                    return value
                except Exception:
                    return json.dumps({"raw": value})

        # TEXT (or default)
        if isinstance(col_type, Text):
            return str(value)

        # If unknown type, just return as-is
        return value

    except Exception as e:
        log_event(
            f"Conversion failed for {col_name} → {col_type} with value={value}: {e}",
            level="error",
            column=col_name,
            file=file,
            error=str(e),
        )