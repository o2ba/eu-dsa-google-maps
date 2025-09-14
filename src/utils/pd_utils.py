"""
Pandas utils
"""

def df_from_parquet(path: str):
    import pandas as pd

    df = pd.read_parquet(path)
    return df