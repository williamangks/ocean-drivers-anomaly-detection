#!/usr/bin/env python3
"""
Ingest Chlorophyll-a (8-day composite) via ERDDAP into BigQuery standard.chl_8day.

Dataset:
- ERDDAP griddap dataset: erdMBchla8day_LonPM180
- Variable: chlorophyll (units: mg m-3)
- Longitude range: -180..180 (LonPM180), so no 0..360 conversion is needed. :contentReference[oaicite:3]{index=3}

Time handling:
- ERDDAP 'time' is "Centered Time" for each 8-day composite. :contentReference[oaicite:4]{index=4}
- We convert to a period window:
    period_start_date = center_date - 3 days
    period_end_date   = center_date + 4 days
- We query through month_end + 7 days to ensure end-of-month composites are fetched (centered timestamps).
- We filter rows to periods overlapping the target month: period_start_date <= month_end AND period_end_date >= month_start.
- Because composites cover 8-day windows, the filtered rows may start in the previous month or end in the next month.

This convention is documented and consistent for downstream alignment.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import time
import urllib.request
from pathlib import Path

import pandas as pd
import numpy as np
import xarray as xr

from src.ingest.helpers.syslogging import make_logger, LogFn
from src.ingest.helpers.regions import BoundBox, load_regions
from src.ingest.helpers.dates import month_range
from src.ingest.helpers.netcdf import ensure_local_netcdf
from src.ingest.helpers.bigquery import load_to_bigquery, delete_existing_rows
from src.ingest.helpers.df_validate import require_columns, require_non_nulls
from src.ingest.helpers.xr_utils import standardize_lat_lon

DEFAULT_MIN_BYTES = 1024
MAX_DOWNLOAD_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 2
PS = 'period_start_date'
PE = 'period_end_date'

RAW_REQUIRED_COLS = {'time', 'lat', 'lon', 'chl_mg_m3'}

BQ_SCHEMA = [
    ('period_start_date', 'DATE', 'REQUIRED'),
    ('period_end_date', 'DATE', 'REQUIRED'),
    ('region_id', 'STRING', 'REQUIRED'),
    ('lat', 'FLOAT64', 'REQUIRED'),
    ('lon', 'FLOAT64', 'REQUIRED'),
    ('chl_mg_m3', 'FLOAT64', 'NULLABLE'),
    ('source', 'STRING', 'NULLABLE'),
    ('ingested_at', 'TIMESTAMP', 'NULLABLE'),
]

REQUIRED_FIELDS = [name for name, _, mode in BQ_SCHEMA if mode == 'REQUIRED']
STANDARD_COLS = [name for name, _, _ in BQ_SCHEMA]
STANDARD_COLS_SET = set(STANDARD_COLS)

ERDDAP_BASE = 'https://coastwatch.pfeg.noaa.gov/erddap/griddap'
DATASET_ID = 'erdMBchla8day_LonPM180'
VAR_NAME = 'chlorophyll'
VAR_NAME_SHORTENED = 'chl'
SOURCE_NAME = 'NOAA_ERDDAP_erdMBchla8day_LonPM180'

def build_erddap_nc_url(date_start: dt.date, date_end: dt.date, bb: BoundBox) -> str:
    """
    griddap constraint order:
        chlorophyll[(time)][(altitude)][(latitude)][(longitude)]
    altitude is 0.0.
    """
    t0 = f'{date_start.isoformat()}T00:00:00Z'
    t1 = f'{date_end.isoformat()}T00:00:00Z'

    lat_min, lat_max = sorted([bb.lat_min, bb.lat_max])
    lon_min, lon_max = sorted([bb.lon_min, bb.lon_max])

    constraint = (
        f'{VAR_NAME}'
        f'[({t0}):1:({t1})]'
        f'[(0.0):1:(0.0)]'
        f'[({lat_min}):1:({lat_max})]'
        f'[({lon_min}):1:({lon_max})]'
    )

    return f'{ERDDAP_BASE}/{DATASET_ID}.nc?{constraint}'

def validate_raw_dataframe(df: pd.DataFrame) -> None:
    """
    Validate the raw dataframe produced from the NetCDF before we reshape it
    into the standardized BigQuery-ready schema.
    """
    require_columns(df, RAW_REQUIRED_COLS, label="chl raw_df")

def validate_standardized_dataframe(df: pd.DataFrame) -> None:
    """
    Validate the standardized dataframe matches the expected BigQuery schema:
    required columns exist and required fields are non-null.
    """    
    require_columns(df, STANDARD_COLS_SET, label="chl standardized_df")
    require_non_nulls(df, REQUIRED_FIELDS, label="chl standardized_df")

def subset_to_long(ds: xr.Dataset, region_id: str, log: LogFn) -> pd.DataFrame:
    da = ds[VAR_NAME]

    log(f'var.sizes={dict(da.sizes)} expected_cells={da.size:,}', level='DEBUG')

    # Drop altitude dimension if exists
    if 'altitude' in da.dims:
        da = da.sel(altitude=0.0, drop=True)

    # Fill value
    fill_value = da.attrs.get('_FillValue', da.attrs.get('missing_value'))

    # Rename coords to standard names
    da = standardize_lat_lon(da)
    
    raw_df = da.to_dataframe(name='chl_mg_m3').reset_index()
    validate_raw_dataframe(raw_df)

    center_ts = pd.to_datetime(raw_df['time'], utc=True)
    raw_df[PS] = (center_ts - pd.Timedelta(days=3)).dt.date
    raw_df[PE] = (center_ts + pd.Timedelta(days=4)).dt.date
    raw_df.drop(columns=['time'], inplace=True)

    raw_df['region_id'] = region_id
    raw_df['source'] = SOURCE_NAME
    raw_df['ingested_at'] = pd.Timestamp.now(tz='UTC')

    df = raw_df.loc[:, STANDARD_COLS]
    df = df.assign(chl_mg_m3=df['chl_mg_m3'])

    log(f'raw_df_rows={len(raw_df):,} df_rows={len(df):,}', level='INFO')

    # Apply fill / missing value
    if fill_value is not None:
        try:
            fv = float(fill_value)
            df.loc[df['chl_mg_m3'] == fv, 'chl_mg_m3'] = np.nan
        except (TypeError, ValueError):
            log(
                f'warn: unexpected fill_value={fill_value!r}; using fallback threshold',
                level='DEBUG'
            )
            df.loc[df['chl_mg_m3'] <= -9990000.0, 'chl_mg_m3'] = np.nan
    else:
        df.loc[df['chl_mg_m3'] <= -9990000.0, 'chl_mg_m3'] = np.nan

    # Ensure numeric dtype; coerce invalid -> NaN
    df.loc[:, 'chl_mg_m3'] = pd.to_numeric(df['chl_mg_m3'], errors='coerce')

    validate_standardized_dataframe(df)
    return df

def filter_overlap_month(df: pd.DataFrame, d0: dt.date, d1: dt.date) -> pd.DataFrame:
    return df[(df[PS] <= d1) & (df[PE] >= d0)].copy()

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Ingest chlorophyll-a 8-day composites via ERDDAP into BigQuery.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--regions_yaml',
                        default=os.getenv('REGIONS_YAML', 'src/config/regions.yaml'),
                        help='Path to regions YAML config (env: REGIONS_YAML)')
    parser.add_argument('--region_id', required=True,
                        help='Region key in regions.yaml (example: NTT)')
    parser.add_argument('--year', type=int, required=True,
                        help='Target year (UTC)')
    parser.add_argument('--month', type=int, required=True,
                        help='Target month 1-12 (UTC)')
    parser.add_argument('--bq_project',
                        default=os.getenv('BQ_PROJECT'),
                        required=os.getenv('BQ_PROJECT') is None,
                        help='BigQuery project ID (env: BQ_PROJECT)')
    parser.add_argument('--bq_dataset',
                        default=os.getenv('BQ_DATASET', 'standard'),
                        help='BigQuery dataset name (env: BQ_DATASET)')
    parser.add_argument('--bq_table',
                        default=os.getenv('BQ_TABLE', 'chl_8day'),
                        help='BigQuery table name (env: BQ_TABLE)')
    parser.add_argument('--dry_run', action='store_true',
                        help='Download + parse only; do not delete/load to BigQuery')
    parser.add_argument('--out_dir',
                        default=os.getenv('OUT_DIR', 'data/tmp'),
                        help='Directory to store downloaded NetCDF files (env: OUT_DIR)')
    parser.add_argument('--replace', action='store_true',
                        help='Delete existing overlapping rows for month window before loading')
    parser.add_argument('--force_download', action='store_true',
                        help='Re-download NetCDF even if cached file exists')
    parser.add_argument('--min_bytes', type=int, default=DEFAULT_MIN_BYTES,
                        help='Minimum bytes for NetCDF validation')
    parser.add_argument('--log_row_stats', action='store_true',
                        help='Log min/max dates and counts after filtering')
    parser.add_argument('--query_end_pad_days', type=int, default=7,
                        help='Extra days beyond month_end to include centered composites')
    parser.add_argument('--log_level', default='INFO', choices=['ERROR', 'INFO', 'DEBUG'],
                        help='Logging verbosity')
    args = parser.parse_args()

    log = make_logger(args.log_level, VAR_NAME_SHORTENED)

    if not (1 <= args.month <= 12):
        parser.error('--month must be between 1 and 12')

    regions = load_regions(args.regions_yaml)
    if args.region_id not in regions:
        known = ', '.join(sorted(regions.keys()))
        raise SystemExit(f'Unknown --region_id {args.region_id!r}. Known region_id values: {known}')
    bb = regions[args.region_id]

    d0, d1 = month_range(args.year, args.month)
    query_end = d1 + dt.timedelta(days=args.query_end_pad_days) # pad for centered composites

    log(
        f'start region={args.region_id} period={d0}..{d1} '
        f'query_end_pad_days={args.query_end_pad_days} query_end={query_end} '
        f'dry_run={args.dry_run} replace={args.replace} '
        f'force_download={args.force_download} min_bytes={args.min_bytes}',
        level='INFO'
    )

    url = build_erddap_nc_url(d0, query_end, bb)
    log(f'fetch_url={url}', level='DEBUG')

    out_dir = Path(args.out_dir)
    local_nc = out_dir / f'chl_{args.region_id}_{args.year}_{args.month:02d}.nc'
    log(f'download_path={local_nc}', level='DEBUG')

    ensure_local_netcdf(url, local_nc, force_download=args.force_download, min_bytes=args.min_bytes, log=log)

    with xr.open_dataset(local_nc) as ds:
        df = subset_to_long(ds, args.region_id, log)

    # Keep only composites whose period overlaps the target month window
    rows_before = len(df) # count rows before filter
    df = filter_overlap_month(df, d0, d1)
    log(f'filtered_rows before={rows_before:,} after={len(df):,}', level='DEBUG')

    if args.log_row_stats:
        if len(df) > 0:
            log(
                'row_stats '
                f'rows={len(df):,} '
                f'min_start={df[PS].min()} '
                f'max_end={df[PE].max()} '
                f'unique_windows={df[[PS, PE]].drop_duplicates().shape[0]}',
                level='INFO'
            )
        else:
            log('row_stats rows=0 (after filtering)', level='INFO')

    if args.dry_run:
        log('dry_run: skipped BigQuery load.')
        return

    table_id = f'{args.bq_project}.{args.bq_dataset}.{args.bq_table}'
    log(f'target_table={table_id}', level='INFO')

    if args.replace:
        log(f'replace=true delete_existing table={table_id} region={args.region_id} period={d0}..{d1}')
        delete_existing_rows(args.bq_project, args.bq_dataset, args.bq_table, args.region_id, d0, d1)
    else:
        log('replace=false (append only)')

    # Safety fallback when nulls produced in REQUIRED fields
    if df[REQUIRED_FIELDS].isna().any().any():
        bad = df[df[REQUIRED_FIELDS].isna().any(axis=1)].head(10)
        raise ValueError(f'Nulls in REQUIRED fields:\n{bad}')

    log(f'load_bq table={table_id} rows={len(df):,}')
    load_to_bigquery(df, args.bq_project, args.bq_dataset, args.bq_table, BQ_SCHEMA)
    log(f'done table={table_id}')

if __name__ == '__main__':
    main()
