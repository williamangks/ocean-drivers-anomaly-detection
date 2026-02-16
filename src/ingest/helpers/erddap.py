#!/usr/bin/env python3

from __future__ import annotations

import datetime as dt
from typing import Sequence
from urllib.parse import quote


def utc_day_bounds(
    date_start: dt.date,
    date_end: dt.date,
    *,
    end_exclusive: bool,
    pad_days: int = 0,
) -> tuple[str, str]:
    """
    Return (t0, t1) ISO8601 UTC timestamps aligned to 00:00:00Z.

    Designed for ERDDAP griddap time constraints.

    - t0 is always date_start at 00:00Z
    - If end_exclusive=True, t1 is (date_end + 1 + pad_days) at 00:00Z
      (i.e., query interval [t0, t1) in UTC days)
    - If end_exclusive=False, t1 is (date_end + pad_days) at 00:00Z
    """
    t0 = f"{date_start.isoformat()}T00:00:00Z"
    end = date_end + dt.timedelta(days=pad_days + (1 if end_exclusive else 0))
    t1 = f"{end.isoformat()}T00:00:00Z"
    return t0, t1


def quote_erddap_url(url: str) -> str:
    """
    Quote only the query portion of an ERDDAP URL.

    ERDDAP URLs commonly contain parentheses, brackets, colons, commas, etc.
    We avoid quoting the base URL to prevent breaking the endpoint host/path.
    """
    base, _, query = url.partition("?")
    if not query:
        return url
    safe_query = quote(query, safe="=:/?&()[]%,.;-_T+Z")
    return base + "?" + safe_query


def lon_to_360(lon: float) -> float:
    """Convert lon to [0, 360)."""
    return (lon + 360.0) % 360.0


def lon_intervals_360(lon_min_deg: float, lon_max_deg: float) -> list[tuple[float, float]]:
    """
    Return 1 or 2 lon intervals in [0, 360] representing the bbox.

    If after conversion a > b, it wraps (crosses the dateline), so we split into:
      [a..360] + [0..b]
    """
    a = lon_to_360(lon_min_deg)
    b = lon_to_360(lon_max_deg)
    if a <= b:
        return [(a, b)]
    return [(a, 360.0), (0.0, b)]


def build_griddap_dims(
    *,
    t0: str,
    t1: str,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    include_singleton_dim: bool,
    singleton_value: float = 0.0,
) -> str:
    """
    Build griddap constraint dims in the common order:
        [(time)][(singleton dim like depth/altitude/zlev?)][(lat)][(lon)]
    """
    dims = f"[({t0}):1:({t1})]"
    if include_singleton_dim:
        dims += f"[({singleton_value}):1:({singleton_value})]"
    dims += f"[({lat_min}):1:({lat_max})][({lon_min}):1:({lon_max})]"
    return dims


def build_griddap_nc_url(*, base: str, dataset_id: str, variables: Sequence[str], dims: str) -> str:
    """Build a griddap .nc URL for one or more variables using the same dims for each."""
    query = ",".join([f"{v}{dims}" for v in variables])
    return f"{base}/{dataset_id}.nc?{query}"


def build_griddap_nc_url_one(*, base: str, dataset_id: str, variable: str, dims: str) -> str:
    """Build a griddap .nc URL for a single variable."""
    return f"{base}/{dataset_id}.nc?{variable}{dims}"
