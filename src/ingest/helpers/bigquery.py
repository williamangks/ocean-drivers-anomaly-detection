#!/usr/bin/env python3

import datetime as dt
import pandas as pd
from google.cloud import bigquery

def load_to_bigquery(
    df: pd.DataFrame,
    project: str,
    dataset: str,
    table: str,
    bq_schema,
) -> None:
    """
    Append a DataFrame into an existing BigQuery table using the table's schema.
    """

    client = bigquery.Client(project=project)
    table_id = f'{project}.{dataset}.{table}'
    
    # Explicit schema, so that loads stable and data contract documented
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[bigquery.SchemaField(name, typ, mode=mode) for name, typ, mode in bq_schema]
    )
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

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
    client = bigquery.Client(project=project)
    table_id = f'{project}.{dataset}.{table}'

    sql = f"""
    DELETE FROM `{table_id}`
    WHERE region_id = @region_id
      AND date BETWEEN @d0 AND @d1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('region_id', 'STRING', region_id),
            bigquery.ScalarQueryParameter('d0', 'DATE', date_start),
            bigquery.ScalarQueryParameter('d1', 'DATE', date_end),
        ]
    )
    client.query(sql, job_config=job_config).result()

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
    client = bigquery.Client(project=project)
    table_id = f'{project}.{dataset}.{table}'

    sql = f"""
    DELETE FROM `{table_id}`
    WHERE region_id = @region_id
      AND period_start_date <= @d1
      AND period_end_date >= @d0
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('region_id', 'STRING', region_id),
            bigquery.ScalarQueryParameter('d0', 'DATE', date_start),
            bigquery.ScalarQueryParameter('d1', 'DATE', date_end),
        ]
    )
    client.query(sql, job_config=job_config).result()

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
    client = bigquery.Client(project=project)
    table_id = f'{project}.{dataset}.{table}'

    sql = f"""
    DELETE FROM `{table_id}`
    WHERE region_id = @region_id
      AND date BETWEEN @d0 AND @d1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('region_id', 'STRING', region_id),
            bigquery.ScalarQueryParameter('d0', 'DATE', date_start),
            bigquery.ScalarQueryParameter('d1', 'DATE', date_end),
        ]
    )
    client.query(sql, job_config=job_config).result()
