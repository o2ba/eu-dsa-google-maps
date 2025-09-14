from utils.convert_values import convert_value
import pandas as pd
from utils.logger import log_event
from upload.model import get_model_schema

def normalize_df(df: pd.DataFrame, model_cls, file: str) -> pd.DataFrame:
    schema = get_model_schema(model_cls)

    normalized = {}
    # Handle only known columns
    for col_name, col_type in schema.items():
        if col_name in df.columns:
            normalized[col_name] = df[col_name].apply(
                lambda v: convert_value(v, col_type, col_name, file)
            )
        else:
            # Missing column: fill with None
            normalized[col_name] = [None] * len(df)
            log_event(
                f"Missing column {col_name} in parquet",
                level="warning",
                column=col_name,
                file=file
            )

    # Detect extra columns not in model
    for col in df.columns:
        if col not in schema:
            log_event(
                f"Unexpected column found in parquet: {col}",
                level="warning",
                column=col,
                file=file
            )

    return pd.DataFrame(normalized)