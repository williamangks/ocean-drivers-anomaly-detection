#!/usr/bin/env python3
from __future__ import annotations

from typing import Iterable

import pandas as pd

def coerce_df_to_schema(df: pd.DataFrame, schema: Iterable[tuple[str, str, str]]) -> pd.DataFrame:
    """
    Coerce DataFrame columns to match BigQuery schema types as best as possible.

    Notes:
    - This is NOT validation (required/null checks happen elsewhere).
    - Goal is load stability: avoid object dtype surprises in load_table_from_dataframe.

    schema: iterable of (name, bq_type, mode)
    """
    df = df.copy()

    for name, bq_type, _mode in schema:
        if name not in df.columns:
            continue

        t = bq_type.upper()

        if t == "DATE":
            df[name] = pd.to_datetime(df[name], errors="coerce").dt.date

        elif t == "TIMESTAMP":
            df[name] = pd.to_datetime(df[name], errors="coerce", utc=True)

        elif t in ("FLOAT64", "NUMERIC", "BIGNUMERIC"):
            df[name] = pd.to_numeric(df[name], errors="coerce")

        elif t == "INT64":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")

        elif t == "STRING":
            df[name] = df[name].astype("string").where(df[name].notna(), pd.NA)

        else:
            pass

    return df
