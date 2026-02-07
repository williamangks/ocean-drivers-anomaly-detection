#!/usr/bin/env python3

"""
Ingest NOAA OISST v2.1 daily SST via ERDDAP into BigQuery standard.sst_daily,
"""

from __future__ import annotations

import argparse
import datetime as dt
import urllib.request
from urllib.parse import quote
from pathlib import Path

import pandas as pd
import numpy as np
import xarray as xr

from src.ingest.helpers.syslogging import make_logger, LogFn
from src.ingest.helpers.regions import BoundBox, load_regions
from src.ingest.helpers.dates import month_range
from src.ingest.helpers.netcdf import ensure_local_netcdf, validate_netcdf_file
from src.ingest.helpers.bigquery import load_to_bigquery, delete_existing_rows
from src.ingest.helpers.df_validate import require_columns, require_non_nulls
from src.ingest.helpers.erddap import quote_erddap_url
from src.ingest.helpers.xr_utils import standardize_lat_lon

DEFAULT_MIN_BYTES = 1024

BQ_SCHEMA = [
    ('date', 'DATE', 'REQUIRED'),
    ('region_id', 'STRING', 'REQUIRED'),
    ('lat', 'FLOAT64', 'REQUIRED'),
    ('lon', 'FLOAT64', 'REQUIRED'),
    ('sst_c', 'FLOAT64', 'NULLABLE'),
    ('source', 'STRING', 'REQUIRED'),
    ('ingested_at', 'TIMESTAMP', 'REQUIRED'),
]

RAW_REQUIRED_COLS = {'time', 'lat', 'lon', 'sst_c'}
STANDARD_COLS = ['date', 'region_id', 'lat', 'lon', 'sst_c', 'source', 'ingested_at']
STANDARD_COLS_SET = set(STANDARD_COLS)
REQUIRED_COLS = ['date', 'region_id', 'lat', 'lon', 'source', 'ingested_at']

ERDDAP_BASE = 'https://coastwatch.pfeg.noaa.gov/erddap/griddap'
DATASET_ID = 'ncdcOisst21Agg'
VAR_NAME = 'sst'
SOURCE_NAME = 'NOAA_OISST_v2_1_via_ERDDAP'

def lon_to_360(lon: float) -> float:
    # ERDDAP OISST longitude 0...360 (degrees_east)
    return (lon + 360.0) % 360.0

def build_erddap_nc_url(date_start: dt.date, date_end: dt.date, bb: BoundBox) -> str:
    """
    Build a ERDDAP griddap request URL that returns NetCDF (.nc) for a subset.

    griddap constraint order for this dataset is:
        sst[(time)][(zlev)][(latitude)][(longitude)]

    select zlev=0.0 (surface), time range, lat range, lon range.
    """
    lon_min = lon_to_360(bb.lon_min)
    lon_max = lon_to_360(bb.lon_max)

    # ERDDAP expects ISO timestamps
    t0 = f'{date_start.isoformat()}T00:00:00Z'
    t1 = f'{date_end.isoformat()}T00:00:00Z'

    # use lat/lon as increasing order
    lat_min, lat_max = sorted([bb.lat_min, bb.lat_max])
    lon_min, lon_max = sorted([lon_min, lon_max])

    # time stride 1 day; lat/lon stride 1 grid step (0.25degree) by using '::1'
    # zlev is a single value: [0]
    constraint = (
        f'{VAR_NAME}'
        f'[({t0}):1:({t1})]'
        f'[(0.0):1:(0.0)]'
        f'[({lat_min}):1:({lat_max})]'
        f'[({lon_min}):1:({lon_max})]'
    )

    return f'{ERDDAP_BASE}/{DATASET_ID}.nc?{constraint}'

def subset_to_long(ds: xr.Dataset, region_id: str) -> pd.DataFrame:
    """
    Convert xarray dataset to BigQuery-ready dataframe with columns:
        date, region_id, lat, lon, sst_c, source, ingested_at
    """

    da = ds[VAR_NAME]

    # xarray expose it as ds.coords keys.
    da = standardize_lat_lon(da)

    # Convert to dataframe
    raw_df = da.to_dataframe(name='sst_c').reset_index()

    validate_raw_dataframe(raw_df)
    
    # Convert time -> date
    raw_df['date'] = pd.to_datetime(raw_df['time'], utc=True).dt.date
    raw_df.drop(columns=['time'], inplace=True)

    raw_df['region_id'] = region_id
    raw_df['source'] = SOURCE_NAME
    raw_df['ingested_at'] = pd.Timestamp.now(tz='UTC')

    df = raw_df[STANDARD_COLS].copy()

    # Replace fill values (ERDDAP uses _FillValue -9:99 in metadata for sst)
    fill_value = da.attrs.get('_FillValue', da.attrs.get('missing_value'))

    if fill_value is not None:
        try:
            fv = float(fill_value)
            df.loc[df['sst_c'] == fv, 'sst_c'] = np.nan
        except (TypeError, ValueError):
            df.loc[df['sst_c'] <= -9.0, 'sst_c'] = np.nan
    else:
        df.loc[df['sst_c'] <= -9.0, 'sst_c'] = np.nan

    df['sst_c'] = pd.to_numeric(df['sst_c'], errors='coerce')

    validate_standardized_dataframe(df)
    return df

def validate_raw_dataframe(df: pd.DataFrame) -> None:
    """
    Validate the raw dataframe produced from the NetCDF before we reshape it
    into the standardized BigQuery-ready schema.
    """
    require_columns(df, RAW_REQUIRED_COLS, label='sst raw_df')

def validate_standardized_dataframe(df: pd.DataFrame) -> None:
    """
    Validate the standardized dataframe matches the expected BigQuery schema:
    required columns exist and required fields are non-null.
    """
    require_columns(df, STANDARD_COLS_SET, label='sst standardized_df')
    require_non_nulls(df, REQUIRED_COLS, label='sst standardized_df')

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Ingest NOAA OISST v2.1 daily SST via ERDDAP into BigQuery.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--regions_yaml', default='src/config/regions.yaml',
                        help='Path to regions YAML config.')
    parser.add_argument('--region_id', required=True,
                        help='Region id key from regions.yaml (ex: NTT).')
    parser.add_argument('--year', type=int, required=True,
                        help='Year to ingest (ex: 2001).')
    parser.add_argument('--month', type=int, required=True,
                        help='Month to ingest (1-12).')
    parser.add_argument('--bq_project', required=True,
                        help='BigQuery project id.')
    parser.add_argument('--bq_dataset', default='standard',
                        help='BigQuery dataset.')
    parser.add_argument('--bq_table', default='sst_daily',
                        help='BigQuery table.')
    parser.add_argument('--dry_run', action='store_true',
                        help='Fetch + transform only; skip BigQuery load.')
    parser.add_argument('--out_dir', default='data/tmp',
                        help='Local directory for downloaded NetCDF files.')
    parser.add_argument('--replace', action='store_true',
                        help='Delete existing rows for region+month before loading (idempotent).')
    parser.add_argument('--force_download', action='store_true',
                        help='Re-download the NetCDF even if a valid local file exists.')
    parser.add_argument('--min_bytes', type=int, default=DEFAULT_MIN_BYTES,
                        help='Minimum size (bytes) for cached NetCDF to be considered valid.')
    parser.add_argument('--log_level', default='INFO', choices=['ERROR', 'INFO', 'DEBUG'],
                        help='Logging verbosity')
    args = parser.parse_args()

    log = make_logger(args.log_level, VAR_NAME)

    if not (1 <= args.month <= 12):
        parser.error('--month must be between 1 and 12')

    if args.year < 1980 or args.year > dt.date.today().year:
        parser.error("--year seems out of range")

    regions = load_regions(args.regions_yaml)
    bb = regions[args.region_id]

    d0, d1 = month_range(args.year, args.month)
    log(f'start region={args.region_id} period={d0}..{d1} dry_run={args.dry_run} '
        f'replace={args.replace} force_download={args.force_download} min_bytes={args.min_bytes}')

    url = build_erddap_nc_url(d0, d1, bb)
    log(f'fetch_url={url}')
    
    out_dir = Path(args.out_dir)
    local_nc = out_dir / f'sst_{args.region_id}_{args.year}_{args.month:02d}.nc'
    log(f'download_path={local_nc}')

    ensure_local_netcdf(url, local_nc, force_download=args.force_download, min_bytes=args.min_bytes, log=log)

    with xr.open_dataset(local_nc) as ds:
        df = subset_to_long(ds, args.region_id)
    log(f'rows_ready={len(df):,}')

    if args.dry_run:
        log('dry_run: skipped BigQuery load.')
    else:
        table_id = f'{args.bq_project}.{args.bq_dataset}.{args.bq_table}'

        if args.replace:
            log(f'replace=true delete_existing table={table_id} region={args.region_id} period={d0}..{d1}')
            delete_existing_rows(args.bq_project, args.bq_dataset, args.bq_table, args.region_id, d0, d1)
        else:
            log('replace=false (append only)')

        log(f'load_bq table={table_id} rows={len(df):,}')
        load_to_bigquery(df, args.bq_project, args.bq_dataset, args.bq_table, BQ_SCHEMA)
        log(f'done table={table_id}')

if __name__ == '__main__':
    main()
