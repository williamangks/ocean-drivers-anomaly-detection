#!/usr/bin/env python3
# src/ingest/chl.py

"""
Ingest chlorophyll-a (8-day composite) via ERDDAP into BigQuery standard.chl_8day.

Time handling:
- ERDDAP `time` is a centered timestamp for each 8-day composite.
- Convert centered date to an 8-day window:
    period_start_date = center_date - 3 days
    period_end_date   = center_date + 4 days
- Query beyond month_end by pad_days to capture late-month composites.
- Keep rows whose composite window overlaps the month:
    period_start_date <= month_end AND period_end_date >= month_start
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path

import pandas as pd
import xarray as xr

from src.ingest.helpers.bigquery import delete_existing_chl_rows, load_to_bigquery
from src.ingest.helpers.dates import month_range
from src.ingest.helpers.df_validate import require_columns, require_non_nulls
from src.ingest.helpers.erddap import build_griddap_dims, build_griddap_nc_url_one, utc_day_bounds
from src.ingest.helpers.netcdf import ensure_local_netcdf
from src.ingest.helpers.regions import BoundBox, load_regions
from src.ingest.helpers.syslogging import LogFn, make_logger
from src.ingest.helpers.xr_utils import apply_fill_to_nan, standardize_lat_lon

DEFAULT_MIN_BYTES = 1024
PS = "period_start_date"
PE = "period_end_date"

ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"
DATASET_ID = "erdMBchla8day_LonPM180"
SRC_VAR = "chlorophyll"
OUT_VAR = "chl_mg_m3"
DRIVER_NAME = "chl"
SOURCE_NAME = "NOAA_ERDDAP_erdMBchla8day_LonPM180"

RAW_REQUIRED_COLS = {"time", "lat", "lon", OUT_VAR}

BQ_SCHEMA = [
    ("period_start_date", "DATE", "REQUIRED"),
    ("period_end_date", "DATE", "REQUIRED"),
    ("region_id", "STRING", "REQUIRED"),
    ("lat", "FLOAT64", "REQUIRED"),
    ("lon", "FLOAT64", "REQUIRED"),
    ("chl_mg_m3", "FLOAT64", "NULLABLE"),
    ("source", "STRING", "REQUIRED"),
    ("ingested_at", "TIMESTAMP", "REQUIRED"),
]

STANDARD_COLS = [name for name, _, _ in BQ_SCHEMA]
STANDARD_COLS_SET = set(STANDARD_COLS)
REQUIRED_COLS = [name for name, _, mode in BQ_SCHEMA if mode == "REQUIRED"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest chlorophyll-a 8-day composites via ERDDAP into BigQuery.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--regions_yaml", default=os.getenv("REGIONS_YAML", "src/config/regions.yaml"))
    p.add_argument("--region_id", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--bq_project", default=os.getenv("BQ_PROJECT"), required=os.getenv("BQ_PROJECT") is None)
    p.add_argument("--bq_dataset", default=os.getenv("BQ_DATASET", "standard"))
    p.add_argument("--bq_table", default=os.getenv("BQ_TABLE", "chl_8day"))
    p.add_argument("--dry_run", action="store_true")
    p.add_argument("--out_dir", default=os.getenv("OUT_DIR", "data/tmp"))
    p.add_argument("--replace", action="store_true")
    p.add_argument("--force_download", action="store_true")
    p.add_argument("--min_bytes", type=int, default=DEFAULT_MIN_BYTES)
    p.add_argument("--log_row_stats", action="store_true")
    p.add_argument("--query_end_pad_days", type=int, default=7)
    p.add_argument("--log_level", default="INFO", choices=["ERROR", "INFO", "DEBUG"])
    return p.parse_args()


def validate_raw_dataframe(df: pd.DataFrame) -> None:
    require_columns(df, RAW_REQUIRED_COLS, label="chl raw_df")


def validate_standardized_dataframe(df: pd.DataFrame) -> None:
    require_columns(df, STANDARD_COLS_SET, label="chl standardized_df")
    require_non_nulls(df, REQUIRED_COLS, label="chl standardized_df")


def build_chl_erddap_url(d0: dt.date, d1: dt.date, bb: BoundBox, *, pad_days: int) -> str:
    """
    Build a griddap NetCDF URL for chlorophyll subset.

    Dataset notes:
    - Longitude is -180..180 (no 0..360 conversion).
    - Includes a singleton altitude dimension at 0.0.

    Time window:
    - Query end is extended by pad_days and is end-exclusive:
      [d0 00:00Z, (d1 + 1 + pad_days) 00:00Z)
    """
    t0, t1 = utc_day_bounds(d0, d1, end_exclusive=True, pad_days=pad_days)

    lat_min, lat_max = sorted([bb.lat_min, bb.lat_max])
    lon_min, lon_max = sorted([bb.lon_min, bb.lon_max])

    dims = build_griddap_dims(
        t0=t0,
        t1=t1,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
        include_singleton_dim=True,  # altitude slice at 0.0
        singleton_value=0.0,
    )
    return build_griddap_nc_url_one(base=ERDDAP_BASE, dataset_id=DATASET_ID, variable=SRC_VAR, dims=dims)


def subset_to_long(ds: xr.Dataset, *, region_id: str, log: LogFn) -> pd.DataFrame:
    da = standardize_lat_lon(ds[SRC_VAR])

    if "altitude" in da.dims:
        da = da.sel(altitude=0.0, drop=True)

    da = apply_fill_to_nan(da)

    raw_df = da.to_dataframe(name=OUT_VAR).reset_index()
    validate_raw_dataframe(raw_df)

    center_ts = pd.to_datetime(raw_df["time"], utc=True)
    raw_df[PS] = (center_ts - pd.Timedelta(days=3)).dt.date
    raw_df[PE] = (center_ts + pd.Timedelta(days=4)).dt.date
    raw_df.drop(columns=["time"], inplace=True)

    raw_df["region_id"] = region_id
    raw_df["source"] = SOURCE_NAME
    raw_df["ingested_at"] = pd.Timestamp.now(tz="UTC")

    raw_df[OUT_VAR] = pd.to_numeric(raw_df[OUT_VAR], errors="coerce")

    df = raw_df.loc[:, STANDARD_COLS].copy()
    validate_standardized_dataframe(df)

    log(f"rows_ready={len(df):,}", level="INFO")
    return df


def filter_overlap_month(df: pd.DataFrame, d0: dt.date, d1: dt.date) -> pd.DataFrame:
    return df[(df[PS] <= d1) & (df[PE] >= d0)].copy()


def log_row_stats(df: pd.DataFrame, log: LogFn) -> None:
    if len(df) == 0:
        log("row_stats rows=0", level="INFO")
        return
    log(
        "row_stats "
        f"rows={len(df):,} "
        f"min_start={df[PS].min()} "
        f"max_end={df[PE].max()} "
        f"unique_windows={df[[PS, PE]].drop_duplicates().shape[0]}",
        level="INFO",
    )


def main() -> None:
    args = parse_args()
    log = make_logger(args.log_level, DRIVER_NAME)

    if not (1 <= args.month <= 12):
        raise SystemExit("--month must be between 1 and 12")

    regions = load_regions(args.regions_yaml)
    if args.region_id not in regions:
        known = ", ".join(sorted(regions.keys()))
        raise SystemExit(f"Unknown --region_id {args.region_id!r}. Known: {known}")
    bb = regions[args.region_id]

    d0, d1 = month_range(args.year, args.month)

    url = build_chl_erddap_url(d0, d1, bb, pad_days=args.query_end_pad_days)
    log(f"fetch_url={url}", level="DEBUG")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    local_nc = out_dir / f"chl_{args.region_id}_{args.year}_{args.month:02d}.nc"
    log(f"download_path={local_nc}", level="DEBUG")

    ensure_local_netcdf(url, local_nc, force_download=args.force_download, min_bytes=args.min_bytes, log=log)

    with xr.open_dataset(local_nc) as ds:
        df = subset_to_long(ds, region_id=args.region_id, log=log)

    df = filter_overlap_month(df, d0, d1)

    if args.log_row_stats:
        log_row_stats(df, log)

    if args.dry_run:
        log("dry_run: skipped BigQuery load.", level="INFO")
        return

    table_id = f"{args.bq_project}.{args.bq_dataset}.{args.bq_table}"

    if args.replace:
        log(f"replace=true delete_existing table={table_id} region={args.region_id} period={d0}..{d1}", level="INFO")
        delete_existing_chl_rows(args.bq_project, args.bq_dataset, args.bq_table, args.region_id, d0, d1)
    else:
        log("replace=false (append only)", level="INFO")

    log(f"load_bq table={table_id} rows={len(df):,}", level="INFO")
    load_to_bigquery(df, args.bq_project, args.bq_dataset, args.bq_table, BQ_SCHEMA)
    log(f"done table={table_id}", level="INFO")


if __name__ == "__main__":
    main()
