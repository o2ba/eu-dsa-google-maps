import json
import uuid
import pandas as pd
import pytz
from utils.logger import log_event
from sqlalchemy import Date, Boolean, Text, String, TIMESTAMP
from snowflake.sqlalchemy import VARIANT


def convert_value(value, col_type, col_name, file):
    """
    Convert a parquet value into a DB-compatible Python object
    based on SQLAlchemy column type for Snowflake.
    """
    try:
        if pd.isna(value):
            return None

        # UUID-like strings (Snowflake stores UUID as STRING(36))
        if isinstance(col_type, String) and getattr(col_type, "length", None) == 36:
            try:
                return str(uuid.UUID(str(value)))  # validate it's a real UUID
            except Exception:
                return str(value)  # fallback: just store as string

        # DATE
        if isinstance(col_type, Date):
            return pd.to_datetime(value, errors="coerce").date()

        # TIMESTAMP_TZ (Snowflake) = TIMESTAMP(timezone=True)
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

        # VARIANT (Snowflake JSON type)
        if isinstance(col_type, VARIANT):
            if isinstance(value, (dict, list)):
                return value
            if isinstance(value, str):
                try:
                    return json.loads(value)  # if it's valid JSON, return dict/list
                except Exception:
                    return {"raw": value}  # fallback: wrap raw string
            return value  # primitives (int/float/bool) are allowed in VARIANT

        # TEXT / STRING
        if isinstance(col_type, (Text, String)):
            return str(value)

        # Default passthrough
        return value

    except Exception as e:
        log_event(
            f"Conversion failed for {col_name} → {col_type} with value={value}: {e}",
            level="error",
            column=col_name,
            file=file,
            error=str(e),
        )
        return None