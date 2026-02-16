#!/usr/bin/env python3

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Sequence

import xarray as xr

def standardize_lat_lon(da: xr.DataArray) -> xr.DataArray:
    """
    Rename common coordinate names to standard: latitude->lat, longitude->lon.
    """
    rename_map: dict[str, str] = {}
    if "latitude" in da.coords:
        rename_map["latitude"] = "lat"
    if "longitude" in da.coords:
        rename_map["longitude"] = "lon"
    if rename_map:
        da = da.rename(rename_map)
    return da

def drop_singleton_dim(da: xr.DataArray, dim: str, value: float) -> xr.DataArray:
    """
    Drop a singleton dimension (like depth/altitude) if present.
    """
    if dim in da.dims:
        return da.sel({dim: value}, drop=True)
    return da

def apply_fill_to_nan(da: xr.DataArray, fallback_max: float = 9e35) -> xr.DataArray:
    """
    Replace fill/missing values with NaN using attrs BEFORE aggregations.

    - Uses _FillValue or missing_value if present.
    - Falls back to filtering huge ERDDAP sentinel fills (>= fallback_max).
    """
    fill_value = da.attrs.get("_FillValue", da.attrs.get("missing_value"))
    if fill_value is not None:
        try:
            fv = float(fill_value)
            da = da.where(da != fv)
        except (TypeError, ValueError):
            da = da.where(da < fallback_max)
    else:
        da = da.where(da < fallback_max)
    return da

@contextmanager
def open_xr_datasets(paths: Sequence[Path]) -> Iterator[List[xr.Dataset]]:
    """
    Context manager to open multiple datasets and ensure they are closed.

    Supports "avoid .load()" pipelines:
    - Keep datasets open while xarray lazily reads during to_dataframe()
    - Close files once conversion is done
    """
    dsets: List[xr.Dataset] = []
    try:
        for p in paths:
            dsets.append(xr.open_dataset(p))
        yield dsets
    finally:
        for ds in dsets:
            try:
                ds.close()
            except Exception:
                pass
