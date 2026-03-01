#!/usr/bin/env python3

from __future__ import annotations

import datetime as dt
import os
from typing import Iterable

import pandas as pd
from google.cloud import bigquery

BQSchema = list[tuple[str, str, str]]

DEFAULT_BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-southeast2")

_CLIENT_CACHE: dict[str, bigquery.Client] = {}

def get_client(project: str) -> bigquery.Client:
    """
    Return a cached BigQuery client per project.

    BigQuery Client objects are safe to reuse within a process and this reduces
    repeated instantiation boilerplate across load/delete/log helpers.
    """
    client = _CLIENT_CACHE.get(project)
    if client is None:
        client = bigquery.Client(project=project)
        _CLIENT_CACHE[project] = client
    return client

def load_to_bigquery(
    df: pd.DataFrame,
    project: str,
    dataset: str,
    table: str,
    bq_schema: BQSchema,
) -> None:
    """
    Append a DataFrame into an existing BigQuery table using the table's schema.
    """

    client = get_client(project)
    table_id = f"{project}.{dataset}.{table}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[bigquery.SchemaField(name, typ, mode=mode) for name, typ, mode in bq_schema]
    )
    client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
        location=DEFAULT_BQ_LOCATION,
    ).result()

def _delete_where(
    *,
    project: str,
    dataset: str,
    table: str,
    sql: str,
    params: Iterable[bigquery.ScalarQueryParameter],
    location: str = DEFAULT_BQ_LOCATION,
) -> None:
    client = get_client(project)
    table_id = f"{project}.{dataset}.{table}"

    job_config = bigquery.QueryJobConfig(query_parameters=list(params))
    client.query(sql.format(table_id=table_id), job_config=job_config, location=location).result()

def delete_existing_sst_rows(
    project: str,
    dataset: str,
    table: str,
    region_id: str,
    date_start: dt.date,
    date_end: dt.date,
) -> None:
    """
    SST idempotency: delete rows for region where date is within [date_start, date_end].
    """
    sql = """
    DELETE FROM `{table_id}`
    WHERE region_id = @region_id
      AND date BETWEEN @d0 AND @d1
    """
    _delete_where(
        project=project,
        dataset=dataset,
        table=table,
        sql=sql,
        params=[
            bigquery.ScalarQueryParameter("region_id", "STRING", region_id),
            bigquery.ScalarQueryParameter("d0", "DATE", date_start),
            bigquery.ScalarQueryParameter("d1", "DATE", date_end),
        ],
    )

def delete_existing_chl_rows(
    project: str,
    dataset: str,
    table: str,
    region_id: str,
    date_start: dt.date,
    date_end: dt.date,
) -> None:
    """
    CHL idempotency: delete rows whose composite window overlaps [date_start, date_end].

    Overlap rule:
      period_start_date <= date_end AND period_end_date >= date_start
    """
    sql = """
    DELETE FROM `{table_id}`
    WHERE region_id = @region_id
      AND period_start_date <= @d1
      AND period_end_date >= @d0
    """
    _delete_where(
        project=project,
        dataset=dataset,
        table=table,
        sql=sql,
        params=[
            bigquery.ScalarQueryParameter("region_id", "STRING", region_id),
            bigquery.ScalarQueryParameter("d0", "DATE", date_start),
            bigquery.ScalarQueryParameter("d1", "DATE", date_end),
        ],
    )

def delete_existing_waves_rows(
    project: str,
    dataset: str,
    table: str,
    region_id: str,
    date_start: dt.date,
    date_end: dt.date,
) -> None:
    """
    Waves idempotency: delete existing rows for region_id within the inclusive date window
    [date_start, date_end]. Used for --replace month reloads.    
    """
    sql = """
    DELETE FROM `{table_id}`
    WHERE region_id = @region_id
      AND date BETWEEN @d0 AND @d1
    """
    _delete_where(
        project=project,
        dataset=dataset,
        table=table,
        sql=sql,
        params=[
            bigquery.ScalarQueryParameter("region_id", "STRING", region_id),
            bigquery.ScalarQueryParameter("d0", "DATE", date_start),
            bigquery.ScalarQueryParameter("d1", "DATE", date_end),
        ],
    )

def _to_rfc3339_utc(ts: dt.datetime) -> str:
    """
    Convert datetime to RFC3339 UTC string BigQuery accepts for TIMESTAMP.
    Example: 2026-02-16T03:04:05.123456Z
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    ts = ts.astimezone(dt.timezone.utc)
    return ts.isoformat(timespec="microseconds").replace("+00:00", "Z")

def log_pipeline_run(
    *,
    project: str,
    run_id: str,
    job_name: str,
    start_ts: dt.datetime,
    end_ts: dt.datetime,
    status: str,
    rows_written: int,
    notes: str,
) -> None:
    """
    Insert one row into ops.pipeline_runs.

    Safety:
    - Never raises.
    - Prints error if logging fails.
    """
    table_id = f"{project}.ops.pipeline_runs"

    if notes and len(notes) > 1000:
        notes = notes[:1000]

    row = {
        "run_id": run_id,
        "job_name": job_name,
        "start_ts": _to_rfc3339_utc(start_ts),
        "end_ts": _to_rfc3339_utc(end_ts),
        "status": status,
        "rows_written": int(rows_written),
        "notes": notes or "",
    }

    try:
        client = get_client(project)
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            print(f"[pipeline_runs] ERROR inserting row: {errors}", flush=True)
    except Exception as e:
        print(f"[pipeline_runs] ERROR logging run: {e}", flush=True)
