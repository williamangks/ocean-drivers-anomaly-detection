#!/usr/bin/env python3

from dataclasses import dataclass
import yaml

@dataclass(frozen=True)
class BoundBox:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

def load_regions(
    path: str
) -> dict[str, BoundBox]:
    """
    Load region bounding boxes from YAML and return mapping region_id -> BoundBox.
    """
    with open(path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    out: dict[str, BoundBox] = {}
    for region_id, spec in cfg['regions'].items():
        bb = spec['boundbox']
        out[region_id] = BoundBox(
            lat_min=float(bb['lat_min']),
            lat_max=float(bb['lat_max']),
            lon_min=float(bb['lon_min']),
            lon_max=float(bb['lon_max']),
        )
    return out
