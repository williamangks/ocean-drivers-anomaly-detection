#!/usr/bin/env python3
"""
helpers/pipeline.py

Reusable pipeline-run wrapper:
- standardizes run_id/start/end/status/notes handling
- always logs into ops.pipeline_runs in a finally block
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Callable, Tuple

from src.ingest.helpers.bigquery import log_pipeline_run
from src.ingest.helpers.syslogging import LogFn


def run_tracked(  # CHANGED: new reusable wrapper (new file)
    *,
    project: str,
    job_name: str,
    log: LogFn,
    fn: Callable[[], Tuple[int, str]],
) -> None:
    """
    Run a job function and always log an ops.pipeline_runs row.

    fn() must return: (rows_written, notes)
    """
    run_id = str(uuid.uuid4())  # CHANGED: moved from each script into one place
    start_ts = dt.datetime.now(dt.timezone.utc)  # CHANGED: moved from each script into one place

    status = "FAILED"
    rows_written = 0
    notes = ""

    try:
        rows_written, notes = fn()
        status = "SUCCESS"
    except Exception as e:
        # CHANGED: centralized error notes formatting; scripts can still override notes if they want
        err_msg = f"{type(e).__name__}: {e}"
        if not notes:
            notes = f"err={err_msg[:500]}"
        raise
    finally:
        end_ts = dt.datetime.now(dt.timezone.utc)  # CHANGED: moved from each script into one place
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
            # CHANGED: wrapper never fails the job due to logging
            print(f"[pipeline_runs] ERROR (script wrapper): {e}", flush=True)
