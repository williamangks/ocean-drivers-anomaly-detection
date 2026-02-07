#!/usr/bin/env python3

from __future__ import annotations

from typing import Iterable
import pandas as pd

def require_columns(
    df: pd.DataFrame,
    required: set[str],
    *,
    label: str
) -> None:
    cols = set(df.columns)
    missing = required - cols
    if missing:
        raise ValueError(
            f'{label} missing columns: {sorted(missing)}. '
            f'Got: {sorted(cols)}'
        )

def require_non_nulls(
    df: pd.DataFrame,
    required_cols: Iterable[str],
    *,
    label: str
) -> None:
    required_cols = list(required_cols)
    if df[required_cols].isna().any().any():
        bad = df[df[required_cols].isna().any(axis=1)].head(10)
        raise ValueError(f'{label} nulls in required columns:\n{bad}')
