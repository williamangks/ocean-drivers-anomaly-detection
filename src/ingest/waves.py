!/usr/bin/env python3
"""
Ingest WaveWatch III (WW3) Global Wave Model via ERDDAP into BigQuery standard.waves_daily.

Dataset:
- ERDDAP griddap dataset: NWW3_Global_Best
- Variables:
    - Thgt: significant wave height (meters)  -> swh_m
    - Tper: peak wave period (second)         -> peak_period_s
- Coords: latitude/longitude; longitude is 0..360 degrees_east.
- Time resolution is ~hourly; aggregate to daily means per (date, lat, lon).

Notes:
- Handles dateline-crossing regions by splitting lon request into up to 2 intervals in 0..360 space.
- ERDDAP query uses an end-exclusive UTC window; we filter rows back to the inclusive month window [d0..d1].
- Supports --no_depth_dim for datasets without depth.
- Applies fill/missing values to NaN BEFORE daily averaging.
- Applies conservative physical-range filters.
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import List, Sequence

import pandas as pd
import xarray as xr

from src.ingest.helpers.syslogging import make_logger, LogFn
from src.ingest.helpers.regions import load_regions, BoundBox
from src.ingest.helpers.dates import month_range
from src.ingest.helpers.netcdf import ensure_local_netcdf
from src.ingest.helpers.bigquery import load_to_bigquery, delete_existing_waves_rows
from src.ingest.helpers.df_validate import require_columns, require_non_nulls
from src.ingest.helpers.erddap import (
    lon_intervals_360,
    utc_day_bounds,
    build_griddap_dims,
    build_griddap_nc_url,
)
from src.ingest.helpers.xr_utils import (
    standardize_lat_lon,
    drop_singleton_dim,
    apply_fill_to_nan,
    open_xr_datasets,
)

DEFAULT_MIN_BYTES = 1024

ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"
DATASET_ID = "NWW3_Global_Best"

SWH_VAR = "Thgt"
TP_VAR = "Tper"
VAR_MAP = {SWH_VAR: "swh_m", TP_VAR: "peak_period_s"}

SOURCE_NAME = "PacIOOS_WW3_Global_via_ERDDAP_NWW3_Global_Best"
DRIVER_NAME = "waves"

VALUE_RANGES = {
    "swh_m": (0.0, 40.0),
    "peak_period_s": (0.0, 60.0),
}

BQ_SCHEMA = [
    ("date", "DATE", "REQUIRED"),
    ("region_id", "STRING", "REQUIRED"),
    ("lat", "FLOAT64", "REQUIRED"),
    ("lon", "FLOAT64", "REQUIRED"),
    ("swh_m", "FLOAT64", "NULLABLE"),
    ("peak_period_s", "FLOAT64", "NULLABLE"),
    ("source", "STRING", "REQUIRED"),
    ("ingested_at", "TIMESTAMP", "REQUIRED"),
]

STANDARD_COLS = [name for name, _, _ in BQ_SCHEMA]
STANDARD_COLS_SET = set(STANDARD_COLS)
REQUIRED_COLS = [name for name, _, mode in BQ_SCHEMA if mode == "REQUIRED"]
RAW_REQUIRED_COLS = {"time", "lat", "lon", *set(VAR_MAP.values())}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest WW3 waves via ERDDAP into BigQuery (daily aggregated).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--regions_yaml", default="src/config/regions.yaml", help="Path to regions YAML config.")
    p.add_argument("--region_id", required=True, help="Region id key from regions.yaml (ex: NTT).")
    p.add_argument("--year", type=int, required=True, help="Year to ingest (UTC).")
    p.add_argument("--month", type=int, required=True, help="Month to ingest (1-12, UTC).")
    p.add_argument("--bq_project", required=True, help="BigQuery project id.")
    p.add_argument("--bq_dataset", default="standard", help="BigQuery dataset.")
    p.add_argument("--bq_table", default="waves_daily", help="BigQuery table.")
    p.add_argument("--dry_run", action="store_true", help="Fetch + transform only; skip BigQuery load.")
    p.add_argument("--out_dir", default="data/tmp", help="Local directory for downloaded NetCDF files.")
    p.add_argument("--replace", action="store_true", help="Delete existing rows for region+month before loading.")
    p.add_argument("--force_download", action="store_true", help="Re-download the NetCDF even if cached exists.")
    p.add_argument("--min_bytes", type=int, default=DEFAULT_MIN_BYTES, help="Minimum bytes for cached NetCDF validity.")
    p.add_argument("--no_depth_dim", action="store_true", help="Do not include a depth dimension slice in ERDDAP query.")
    p.add_argument("--log_row_stats", action="store_true", help="Log min/max dates and counts after transformation")
    p.add_argument("--log_level", default="INFO", choices=["ERROR", "INFO", "DEBUG"], help="Logging verbosity")
    return p.parse_args()


def validate_raw_dataframe(df: pd.DataFrame) -> None:
    require_columns(df, RAW_REQUIRED_COLS, label="waves raw_df")


def validate_standardized_dataframe(df: pd.DataFrame) -> None:
    require_columns(df, STANDARD_COLS_SET, label="waves standardized_df")
    require_non_nulls(df, REQUIRED_COLS, label="waves standardized_df")


def build_0_360_lon_split_urls(
    *,
    base: str,
    dataset_id: str,
    variables: Sequence[str],
    date_start: dt.date,
    date_end: dt.date,
    bb: BoundBox,
    include_singleton_dim: bool,
    singleton_value: float = 0.0,
) -> list[tuple[str, tuple[float, float]]]:
    """
    WW3-specific: dataset lon is 0..360; regions may cross the dateline.

    Build 1 or 2 griddap URLs by splitting the lon interval in 0..360 space.
    Uses an end-exclusive day window: [date_start, date_end + 1 day).

    Returns:
        List[(url, (lon0, lon1))] where (lon0, lon1) is the interval returned by lon_intervals_360().
    """
    t0, t1 = utc_day_bounds(date_start, date_end, end_exclusive=True)

    lat_min, lat_max = sorted([bb.lat_min, bb.lat_max])

    out: list[tuple[str, tuple[float, float]]] = []
    for lo0, lo1 in lon_intervals_360(bb.lon_min, bb.lon_max):
        dims = build_griddap_dims(
            t0=t0,
            t1=t1,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=min(lo0, lo1),
            lon_max=max(lo0, lo1),
            include_depth_dim=include_depth_dim,
            depth_value=depth_value,
        )
        url = build_griddap_nc_url(base=base, dataset_id=dataset_id, variables=variables, dims=dims)
        out.append((url, (lo0, lo1)))

    return out


def _waves_cache_filename(
    *,
    region_id: str,
    year: int,
    month: int,
    idx: int,
    interval: tuple[float, float],
    split: bool,
) -> str:
    lo0, lo1 = interval
    if not split:
        return f"waves_{region_id}_{year}_{month:02d}.nc"
    return f"waves_{region_id}_{year}_{month:02d}_part{idx}_lon{lo0:.1f}-{lo1:.1f}.nc"


def download_intervals(
    *,
    url_specs: list[tuple[str, tuple[float, float]]],
    out_dir: Path,
    region_id: str,
    year: int,
    month: int,
    force_download: bool,
    min_bytes: int,
    log: LogFn,
) -> List[Path]:
    split = len(url_specs) > 1
    log(f"dateline_split={split} intervals={len(url_specs)}", level="INFO")

    paths: List[Path] = []
    for idx, (url, interval) in enumerate(url_specs, start=1):
        lo0, lo1 = interval
        log(f"lon_interval[{idx}]={lo0:.1f}..{lo1:.1f} (0..360)", level="INFO")
        log(f"fetch_url[{idx}]={url}", level="DEBUG")

        local_nc = out_dir / _waves_cache_filename(
            region_id=region_id, year=year, month=month, idx=idx, interval=interval, split=split
        )
        log(f"download_path[{idx}]={local_nc}", level="DEBUG")

        ensure_local_netcdf(url, local_nc, force_download=force_download, min_bytes=min_bytes, log=log)
        paths.append(local_nc)

    return paths


def to_daily_mean_dataset(
    dsets: list[xr.Dataset],
    *,
    var_map: dict[str, str],
    singleton_dim: str | None,
    singleton_value: float,
    value_ranges: dict[str, tuple[float, float]],
) -> xr.Dataset:
    """
    Convert opened datasets into a DAILY-mean dataset.

    Pipeline:
    - Standardize lat/lon coords
    - Optional singleton dim selection (e.g., depth=0)
    - apply_fill_to_nan() before aggregation
    - physical range filters
    - resample(time="1D").mean(skipna=True)
    - concat lon-split pieces along lon and sort
    """
    pieces: list[xr.Dataset] = []

    for ds in dsets:
        data_vars: dict[str, xr.DataArray] = {}

        for src_var, out_var in var_map.items():
            da = standardize_lat_lon(ds[src_var])

            if singleton_dim is not None:
                da = drop_singleton_dim(da, singleton_dim, singleton_value)

            da = apply_fill_to_nan(da)

            vmin, vmax = value_ranges[out_var]
            da = da.where((da >= vmin) & (da <= vmax))

            data_vars[out_var] = da

        ds_daily = xr.Dataset(data_vars).resample(time="1D").mean(skipna=True)
        pieces.append(ds_daily)

    if len(pieces) == 1:
        return pieces[0]

    return xr.concat(pieces, dim="lon").sortby("lon")


def daily_to_dataframe(
    ds_daily: xr.Dataset,
    *,
    region_id: str,
    d0: dt.date,
    d1: dt.date,
    log: LogFn,
) -> pd.DataFrame:
    if "time" not in ds_daily.coords and "time" not in ds_daily.dims:
        raise ValueError("ds_daily has no 'time' coordinate/dimension; cannot convert to daily dataframe")

    raw_df = ds_daily.to_dataframe().reset_index()
    validate_raw_dataframe(raw_df)

    raw_df["date"] = pd.to_datetime(raw_df["time"], utc=True).dt.date
    raw_df.drop(columns=["time"], inplace=True)

    # Filter back to requested inclusive window (query is end-exclusive)
    raw_df = raw_df[(raw_df["date"] >= d0) & (raw_df["date"] <= d1)].copy()

    raw_df["region_id"] = region_id
    raw_df["source"] = SOURCE_NAME
    raw_df["ingested_at"] = pd.Timestamp.now(tz="UTC")

    for col in VAR_MAP.values():
        raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce")

    df = raw_df.loc[:, STANDARD_COLS].copy()
    validate_standardized_dataframe(df)

    log(f"daily_rows={len(df):,}", level="INFO")
    return df


def log_row_stats(df: pd.DataFrame, log: LogFn) -> None:
    if len(df) == 0:
        log("row_stats rows=0", level="INFO")
        return
    log(
        "row_stats "
        f"rows={len(df):,} "
        f"min_date={df['date'].min()} "
        f"max_date={df['date'].max()} "
        f"unique_lat={df['lat'].nunique()} "
        f"unique_lon={df['lon'].nunique()}",
        level="INFO",
    )


def main() -> None:
    args = parse_args()
    log = make_logger(args.log_level, DRIVER_NAME)

    if not (1 <= args.month <= 12):
        raise SystemExit("--month must be between 1 and 12")

    if args.year < 2016 or args.year > dt.date.today().year + 1:
        raise SystemExit("--year seems out of range")

    regions = load_regions(args.regions_yaml)
    if args.region_id not in regions:
        known = ", ".join(sorted(regions.keys()))
        raise SystemExit(f"Unknown --region_id {args.region_id!r}. Known: {known}")
    bb = regions[args.region_id]

    d0, d1 = month_range(args.year, args.month)

    log(
        f"start region={args.region_id} period={d0}..{d1} "
        f"dry_run={args.dry_run} replace={args.replace} "
        f"force_download={args.force_download} min_bytes={args.min_bytes} "
        f"no_depth_dim={args.no_depth_dim} dataset_id={DATASET_ID} vars={list(VAR_MAP.keys())}",
        level="INFO",
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    url_specs = build_0_360_lon_split_urls(
        base=ERDDAP_BASE,
        dataset_id=DATASET_ID,
        variables=list(VAR_MAP.keys()),
        date_start=d0,
        date_end=d1,
        bb=bb,
        include_depth_dim=not args.no_depth_dim,
        depth_value=0.0,
    )

    local_paths = download_intervals(
        url_specs=url_specs,
        out_dir=out_dir,
        region_id=args.region_id,
        year=args.year,
        month=args.month,
        force_download=args.force_download,
        min_bytes=args.min_bytes,
        log=log,
    )

    with open_xr_datasets(local_paths) as dsets:
        ds_daily = to_daily_mean_dataset(
            dsets,
            var_map=VAR_MAP,
            singleton_dim=None if args.no_depth_dim else "depth",
            singleton_value=0.0,
            value_ranges=VALUE_RANGES,
        )
        df = daily_to_dataframe(ds_daily, region_id=args.region_id, d0=d0, d1=d1, log=log)

    log(f"rows_ready={len(df):,}", level="INFO")

    if args.log_row_stats:
        log_row_stats(df, log)

    if args.dry_run:
        log("dry_run: skipped BigQuery load.", level="INFO")
        return

    table_id = f"{args.bq_project}.{args.bq_dataset}.{args.bq_table}"

    if args.replace:
        log(f"replace=true delete_existing table={table_id} region={args.region_id} period={d0}..{d1}", level="INFO")
        delete_existing_waves_rows(args.bq_project, args.bq_dataset, args.bq_table, args.region_id, d0, d1)
    else:
        log("replace=false (append only)", level="INFO")

    if df[REQUIRED_COLS].isna().any().any():
        bad = df[df[REQUIRED_COLS].isna().any(axis=1)].head(10)
        raise ValueError(f"Nulls in REQUIRED fields:\n{bad}")

    log(f"load_bq table={table_id} rows={len(df):,}", level="INFO")
    load_to_bigquery(df, args.bq_project, args.bq_dataset, args.bq_table, BQ_SCHEMA)
    log(f"done table={table_id}", level="INFO")


if __name__ == "__main__":
    main()
