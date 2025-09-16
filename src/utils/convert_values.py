import pandas as pd
import uuid, json, pytz
from sqlalchemy import String, Date, Boolean, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
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
                return str(uuid.UUID(str(value)))
            except Exception:
                return str(value)  # fallback

        # DATE
        if isinstance(col_type, Date):
            # Handles both epoch ints and strings gracefully
            try:
                # integers → assume epoch ms
                if isinstance(value, (int, float)):
                    return pd.to_datetime(value, errors="coerce", unit="ms").date()
                return pd.to_datetime(value, errors="coerce").date()
            except Exception:
                return None

        # TIMESTAMP_TZ (Snowflake) = TIMESTAMP(timezone=True)
        if isinstance(col_type, TIMESTAMP) and col_type.timezone:
            try:
                if isinstance(value, (int, float)):
                    # assume ms since epoch if 13 digits
                    ts = pd.to_datetime(int(value), errors="coerce", unit="ms", utc=True)
                else:
                    ts = pd.to_datetime(value, errors="coerce", utc=True)

                if pd.isna(ts):
                    log_event(
                        f"Invalid timestamp for {col_name}", 
                        level="warning",
                        file=file,
                        column=col_name,
                        bad_value=value
                    )
                    return None
                return ts.to_pydatetime().astimezone(pytz.UTC)
            except Exception as e:
                log_event(
                    f"Timestamp conversion failed for {col_name}: {e}",
                    level="error", column=col_name, file=file, bad_value=value
                )
                return None

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
                    return json.loads(value)
                except Exception:
                    return {"raw": value}
            return value  # primitives passthrough

        # TEXT / STRING (non‑UUID cases)
        if isinstance(col_type, (Text, String)):
            return str(value)

        # Default passthrough
        return value

    except Exception as e:
        # last-resort safety
        from utils.logger import log_event
        log_event(
            f"Conversion failed for {col_name} → {col_type} with value={value}: {e}",
            level="error",
            column=col_name,
            file=file,
            error=str(e),
        )
        return None