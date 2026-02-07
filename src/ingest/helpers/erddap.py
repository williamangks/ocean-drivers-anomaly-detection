#!/usr/bin/env python3

from __future__ import annotations

from urllib.parse import quote

def quote_erddap_url(url: str) -> str:
    """
    ERDDAP URLs often include parentheses, brackets, colons, commas, etc.
    Quote only the query portion so we don't break the base URL.
    """
    base, _, query = url.partition('?')
    if not query:
        return url
    safe_query = quote(query, safe='=:/?&()[]%,.-_T+Z')
    return base + '?' + safe_query
