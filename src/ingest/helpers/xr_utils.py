#!/usr/bin/env python3

from __future__ import annotations

import xarray as xr

def standardize_lat_lon(da: xr.DataArray) -> xr.DataArray:
    """
    Rename common coordinate names to standard: latitude->lat, longitude->lon.
    """
    rename_map: dict[str, str] = {}
    if 'latitude' in da.coords:
        rename_map['latitude'] = 'lat'
    if 'longitude' in da.coords:
        rename_map['longitude'] = 'lon'
    if rename_map:
        da = da.rename(rename_map)
    return da
