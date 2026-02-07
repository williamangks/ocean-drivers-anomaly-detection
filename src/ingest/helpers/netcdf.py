#!/usr/bin/env python3

import time
import shutil
import urllib.request
from urllib.parse import quote
from urllib.error import HTTPError, URLError
from pathlib import Path

DEFAULT_MIN_BYTES = 1024
MAX_DOWNLOAD_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 2

from src.ingest.helpers.erddap import quote_erddap_url

def validate_netcdf_file(
    path: str | Path,
    min_bytes: int = DEFAULT_MIN_BYTES
) -> tuple[bool, str]:
    """
    Validate that a local file looks like a real NetCDF file.
    
    Use:
    - Prevents using cached files that are actually ERDDAP error messages (ASCII),
      partial downloads, or corrupted files.
    - Helps data pipeline decide to reuse cached download or re-download.

    Checks:
    1. file exists
    2. file size >= min_bytes
    3. file header is NetCDF classic (b"CDF") or NetCDF4/HDF5 (b"\\x89HDF")
    """
    p = Path(path)
    if not p.exists():
        return False, f'path={p} missing'

    size = p.stat().st_size
    if size < min_bytes:
        return False, f'path={p} too_small size={size}B min_bytes={min_bytes}'

    with p.open('rb') as f:
        head4 = f.read(4)

    # NetCDF common signature
    if head4.startswith(b'CDF'):
        return True, f'path={p} netcdf_classic size={size}B'

    # NetCDF4 files are HDF5 containers
    if head4 == b'\x89HDF':
        return True, f'path={p} netcdf4_hdf5 size={size}B'

    return False, f'path={p} bad_header head4={head4!r} size={size}B'

def _download_with_urlopen(
    url: str,
    tmp_path: Path,
    timeout: int = 60
) -> None:
    req = urllib.request.Request(url, headers={'User-Agent': 'ocean-drivers-anomaly-detection/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if getattr(r, 'status', 200) != 200:
                raise RuntimeError(f'HTTP status={getattr(r,"status",None)} for url={url}')
            with tmp_path.open('wb') as f:
                shutil.copyfileobj(r, f)
    except HTTPError as e:
        raise RuntimeError(f'HTTPError status={e.code} reason={e.reason} url={url}') from e
    except URLError as e:
        raise RuntimeError(f'URLError reason={e.reason} url={url}') from e

def ensure_local_netcdf(
    url: str,
    local_nc: Path,
    force_download: bool,
    log: LogFn,
    min_bytes: int = DEFAULT_MIN_BYTES
) -> None:
    """
    Ensure a valid NetCDF file exists at `local_nc`.

    1. Reuses cached file if valid (validate_netcdf_file) and force_download=False.
    2. Otherwise downloads from `url`, then validates again.
    3. Raises RuntimeError if the downloaded file is still invalid.
    """
    local_nc.parent.mkdir(parents=True, exist_ok=True)

    # Quoting only the query portion
    safe_url = quote_erddap_url(url)

    ok, info = validate_netcdf_file(local_nc, min_bytes=min_bytes)
    if ok and not force_download:
        log(f'using_cached_download=true ({info})', level='DEBUG')
        return

    reason = info if not ok else 'force_download=true'
    log(f'using_cached_download=false (re-downloading) reason={reason}', level='INFO')

    tmp_path = local_nc.with_suffix(local_nc.suffix + '.part')
    last_err: Exception | None = None

    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        try:
            if tmp_path.exists():
                tmp_path.unlink()
            
            _download_with_urlopen(safe_url, tmp_path, timeout=60)

            ok_tmp, info_tmp = validate_netcdf_file(tmp_path, min_bytes=min_bytes)
            if not ok_tmp:
                raise RuntimeError(f'Downloaded temp file failed validation: {info_tmp}')

            tmp_path.replace(local_nc)            
            last_err = None
            break

        except Exception as e:
            last_err = e

            is_last = attempt == MAX_DOWNLOAD_ATTEMPTS
            level: LogLevel = 'ERROR' if is_last else 'INFO'
            log(f'download_attempt_failed attempt={attempt}/{MAX_DOWNLOAD_ATTEMPTS} err={e}', level=level)

            if not is_last:
                time.sleep(BACKOFF_BASE_SECONDS * attempt)

    if last_err is not None:
        raise RuntimeError(f'Failed to download after retries: {last_err}')

    ok2, info2 = validate_netcdf_file(local_nc, min_bytes=min_bytes)
    if not ok2:
        raise RuntimeError(f'Downloaded file failed validation: {info2}')
    log(f'download_complete=true ({info2})', level='INFO')
