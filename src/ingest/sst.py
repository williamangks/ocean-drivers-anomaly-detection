#!/usr/bin/env python3
# src/ingest/sst.py

"""
Ingest NOAA OISST v2.1 daily SST via ERDDAP into BigQuery standard.sst_daily.
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd
import xarray as xr

from src.ingest.helpers.bigquery import delete_existing_sst_rows, load_to_bigquery
from src.ingest.helpers.dates import month_range
from src.ingest.helpers.df_validate import require_columns, require_non_nulls
from src.ingest.helpers.erddap import build_griddap_dims, build_griddap_nc_url_one, lon_to_360, utc_day_bounds
from src.ingest.helpers.netcdf import ensure_local_netcdf
from src.ingest.helpers.regions import BoundBox, load_regions
from src.ingest.helpers.syslogging import LogFn, make_logger
from src.ingest.helpers.xr_utils import apply_fill_to_nan, standardize_lat_lon

DEFAULT_MIN_BYTES = 1024

ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"
DATASET_ID = "ncdcOisst21Agg"

SRC_VAR = "sst"
OUT_VAR = "sst_c"
SOURCE_NAME = "NOAA_OISST_v2_1_via_ERDDAP"
DRIVER_NAME = "sst"

BQ_SCHEMA = [
    ("date", "DATE", "REQUIRED"),
    ("region_id", "STRING", "REQUIRED"),
    ("lat", "FLOAT64", "REQUIRED"),
    ("lon", "FLOAT64", "REQUIRED"),
    ("sst_c", "FLOAT64", "NULLABLE"),
    ("source", "STRING", "REQUIRED"),
    ("ingested_at", "TIMESTAMP", "REQUIRED"),
]

STANDARD_COLS = [name for name, _, _ in BQ_SCHEMA]
STANDARD_COLS_SET = set(STANDARD_COLS)
REQUIRED_COLS = [name for name, _, mode in BQ_SCHEMA if mode == "REQUIRED"]
RAW_REQUIRED_COLS = {"time", "lat", "lon", OUT_VAR}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest NOAA OISST v2.1 daily SST via ERDDAP into BigQuery.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--regions_yaml", default="src/config/regions.yaml", help="Path to regions YAML config.")
    p.add_argument("--region_id", required=True, help="Region id key from regions.yaml (ex: NTT).")
    p.add_argument("--year", type=int, required=True, help="Year to ingest (UTC).")
    p.add_argument("--month", type=int, required=True, help="Month to ingest (1-12, UTC).")
    p.add_argument("--bq_project", required=True, help="BigQuery project id.")
    p.add_argument("--bq_dataset", default="standard", help="BigQuery dataset.")
    p.add_argument("--bq_table", default="sst_daily", help="BigQuery table.")
    p.add_argument("--dry_run", action="store_true", help="Fetch + transform only; skip BigQuery load.")
    p.add_argument("--out_dir", default="data/tmp", help="Local directory for downloaded NetCDF files.")
    p.add_argument("--replace", action="store_true", help="Delete existing rows for region+month before loading.")
    p.add_argument("--force_download", action="store_true", help="Re-download the NetCDF even if cached exists.")
    p.add_argument("--min_bytes", type=int, default=DEFAULT_MIN_BYTES, help="Minimum bytes for cached NetCDF validity.")
    p.add_argument("--log_level", default="INFO", choices=["ERROR", "INFO", "DEBUG"], help="Logging verbosity")
    return p.parse_args()


def validate_raw_dataframe(df: pd.DataFrame) -> None:
    require_columns(df, RAW_REQUIRED_COLS, label="sst raw_df")


def validate_standardized_dataframe(df: pd.DataFrame) -> None:
    require_columns(df, STANDARD_COLS_SET, label="sst standardized_df")
    require_non_nulls(df, REQUIRED_COLS, label="sst standardized_df")


def build_sst_erddap_url(d0: dt.date, d1: dt.date, bb: BoundBox) -> str:
    """
    Build a griddap NetCDF URL for SST subset for the inclusive window [d0, d1] (UTC days).

    Implementation detail:
    - We use an end-exclusive time bound: [d0 00:00Z, (d1+1) 00:00Z).
    - Dataset longitude is 0..360 degrees_east.
    - Dataset includes a singleton zlev dimension at 0.0 (surface).
    """
    lon_min = lon_to_360(bb.lon_min)
    lon_max = lon_to_360(bb.lon_max)

    # end_exclusive=True => t1 = (d1 + 1) 00:00Z, covering all UTC days in [d0, d1]
    t0, t1 = utc_day_bounds(d0, d1, end_exclusive=True)

    lat_min, lat_max = sorted([bb.lat_min, bb.lat_max])
    lon_min, lon_max = sorted([lon_min, lon_max])

    dims = build_griddap_dims(
        t0=t0,
        t1=t1,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
        include_singleton_dim=True,  # zlev slice at 0.0
        singleton_value=0.0,
    )
    return build_griddap_nc_url_one(base=ERDDAP_BASE, dataset_id=DATASET_ID, variable=SRC_VAR, dims=dims)


def subset_to_long(ds: xr.Dataset, *, region_id: str, log: LogFn) -> pd.DataFrame:
    da = standardize_lat_lon(ds[SRC_VAR])

    if "zlev" in da.dims:
        da = da.sel(zlev=0.0, drop=True)

    da = apply_fill_to_nan(da)

    raw_df = da.to_dataframe(name=OUT_VAR).reset_index()
    validate_raw_dataframe(raw_df)

    raw_df["date"] = pd.to_datetime(raw_df["time"], utc=True).dt.date
    raw_df.drop(columns=["time"], inplace=True)

    raw_df["region_id"] = region_id
    raw_df["source"] = SOURCE_NAME
    raw_df["ingested_at"] = pd.Timestamp.now(tz="UTC")

    raw_df[OUT_VAR] = pd.to_numeric(raw_df[OUT_VAR], errors="coerce")

    df = raw_df.loc[:, STANDARD_COLS].copy()
    validate_standardized_dataframe(df)

    log(f"rows_ready={len(df):,}", level="INFO")
    return df


def main() -> None:
    args = parse_args()
    log = make_logger(args.log_level, DRIVER_NAME)

    if not (1 <= args.month <= 12):
        raise SystemExit("--month must be between 1 and 12")
    if args.year < 1980 or args.year > dt.date.today().year:
        raise SystemExit("--year seems out of range")

    regions = load_regions(args.regions_yaml)
    if args.region_id not in regions:
        known = ", ".join(sorted(regions.keys()))
        raise SystemExit(f"Unknown --region_id {args.region_id!r}. Known: {known}")
    bb = regions[args.region_id]

    d0, d1 = month_range(args.year, args.month)

    log(
        f"start region={args.region_id} period={d0}..{d1} dry_run={args.dry_run} "
        f"replace={args.replace} force_download={args.force_download} min_bytes={args.min_bytes}",
        level="INFO",
    )

    url = build_sst_erddap_url(d0, d1, bb)
    log(f"fetch_url={url}", level="DEBUG")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    local_nc = out_dir / f"sst_{args.region_id}_{args.year}_{args.month:02d}.nc"
    log(f"download_path={local_nc}", level="DEBUG")

    ensure_local_netcdf(url, local_nc, force_download=args.force_download, min_bytes=args.min_bytes, log=log)

    with xr.open_dataset(local_nc) as ds:
        df = subset_to_long(ds, region_id=args.region_id, log=log)

    if args.dry_run:
        log("dry_run: skipped BigQuery load.", level="INFO")
        return

    table_id = f"{args.bq_project}.{args.bq_dataset}.{args.bq_table}"

    if args.replace:
        log(f"replace=true delete_existing table={table_id} region={args.region_id} period={d0}..{d1}", level="INFO")
        delete_existing_sst_rows(args.bq_project, args.bq_dataset, args.bq_table, args.region_id, d0, d1)
    else:
        log("replace=false (append only)", level="INFO")

    log(f"load_bq table={table_id} rows={len(df):,}", level="INFO")
    load_to_bigquery(df, args.bq_project, args.bq_dataset, args.bq_table, BQ_SCHEMA)
    log(f"done table={table_id}", level="INFO")


if __name__ == "__main__":
    main()
