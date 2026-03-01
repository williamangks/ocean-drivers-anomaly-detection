#!/usr/bin/env python3
"""
Reusable pipeline-run wrapper:
- standardizes run_id/start/end/status/notes handling
- always logs into ops.pipeline_runs in a finally block
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Callable

from src.ingest.helpers.bigquery import log_pipeline_run
from src.ingest.helpers.syslogging import LogFn


def run_tracked(
    *,
    project: str,
    job_name: str,
    log: LogFn,
    fn: Callable[[], tuple[int, str]],
) -> None:
    """
    Run a job function and always log an ops.pipeline_runs row.

    fn() must return: (rows_written, notes)
    """
    run_id = str(uuid.uuid4())
    start_ts = dt.datetime.now(dt.timezone.utc)

    status = "FAILED"
    rows_written = 0
    notes = ""

    log(f"run_started run_id={run_id} job={job_name}", level="INFO")
    try:
        rows_written, notes = fn()
        status = "SUCCESS"
    except Exception as e:
        status = "FAILED"

        err_msg = f"{type(e).__name__}: {e}"
        err_kv = f"err={err_msg[:500]}"

        base = f"job={job_name} run_id={run_id}"

        if notes:
            notes = f"{base} {notes} {err_kv}"
        else:
            notes = f"{base} {err_kv}"

        raise
    finally:
        end_ts = dt.datetime.now(dt.timezone.utc)

        try:
            log_pipeline_run(
                project=project,
                run_id=run_id,
                job_name=job_name,
                start_ts=start_ts,
                end_ts=end_ts,
                status=status,
                rows_written=rows_written,
                notes=notes,
            )
        except Exception as e:
            log(f"pipeline_runs_log_failed err={type(e).__name__}: {e}", level="ERROR")
