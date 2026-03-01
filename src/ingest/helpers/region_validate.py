#!/usr/bin/env python3
from __future__ import annotations

from src.ingest.helpers.regions import BoundBox, load_regions

def require_region(path: str, region_id: str) -> BoundBox:
    regions = load_regions(path)
    if region_id not in regions:
        known = ", ".join(sorted(regions.keys()))
        raise SystemExit(f"Unknown --region_id {region_id!r}. Known: {known}")
    return regions[region_id]
