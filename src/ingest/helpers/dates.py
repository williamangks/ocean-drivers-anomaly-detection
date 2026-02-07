#!/usr/bin/env python3

import datetime as dt

def month_range(
    year: int,
    month: int
) -> tuple[dt.date, dt.date]:
    """
    Return inclusive (start_date, end_date) for the given year/month.
    """
    start = dt.date(year, month, 1)
    if month == 12:
        end = dt.date(year + 1, 1, 1) - dt.timedelta(days=1)
    else:
        end = dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    return start, end
