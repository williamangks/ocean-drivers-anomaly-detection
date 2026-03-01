# Ocean Drivers Data Platform (SST + Chlorophyll + Waves)

A practical, cheap/free-tier-friendly **data engineering portfolio project** that ingests and standardizes **ocean environmental drivers** for two regions (**HAWAII** and **NTT, Indonesia**) into **BigQuery (Jakarta region: `asia-southeast2`)**.

This repo aims to be:
- **reproducible** (provision + run from scratch)
- **cheap** (free-tier friendly)
- **production-like** (idempotent runs, retries, caching, validation, run logging)

---

## Current phase
Build a production-like ingestion platform that:
1) downloads ocean driver datasets (SST, chlorophyll, waves) via ERDDAP
2) subsets them by region bounding boxes (HAWAII and NTT)
3) loads standardized tables into BigQuery (Jakarta region `asia-southeast2`)
4) writes one row per run into `ops.pipeline_runs` (SUCCESS/FAILED)

Later phases (not implemented yet):
- per-source **watermarks**
- weekly orchestration (**Cloud Scheduler → Cloud Run Jobs** or GitHub Actions cron)
- modeling / anomaly detection + Looker Studio dashboards

---

## Core Concept

This project combines ocean science data formats with data engineering patterns.

### ERDDAP → NetCDF → Pandas → BigQuery

Each ingestion script (`src/ingest/sst.py`, `chl.py`, `waves.py`) follows the same pattern:

1. Build an **ERDDAP griddap URL** using:
   - region bounding box (`regions.yaml`)
   - time window (year/month)
2. Download a **NetCDF subset** (cached locally in `data/tmp/`)
3. Parse using **xarray**
4. Convert to long-format **pandas DataFrame**
5. Validate schema
6. Load into **BigQuery standard layer**

This keeps ingestion reproducible and idempotent.

---

### Standard Layer, and Ops Layer

The warehouse is intentionally split into two layers:

- **`standard` dataset**
  - Cleaned, standardized environmental measurements
  - Partitioned by date
  - Used for analytics and future ML feature engineering

- **`ops` dataset**
  - Operational tracking tables
  - `ops.pipeline_runs` stores:
    - run_id
    - job_name
    - start/end timestamps
    - SUCCESS/FAILED
    - rows_written
    - notes

This separation mirrors production data platforms:
data tables vs observability tables.

---

### Idempotent Monthly Loads

Each ingestion script supports:

- `--dry_run` → test full pipeline without writing to BigQuery
- `--replace` → delete existing rows for region+month before loading

This ensures:
- safe re-runs
- clean backfills
- no duplicate month partitions

Idempotency is enforced using `DELETE` queries before load (see `helpers/bigquery.py`).

---

## Data sources (implemented)
This project subsets global gridded datasets by region bounding box.

- **SST (daily)**: NOAA OISST v2.1 via ERDDAP dataset `ncdcOisst21Agg`
- **Chlorophyll-a (8-day composite)**: ERDDAP dataset `erdMBchla8day_LonPM180`
- **Waves (hourly → daily mean)**: WaveWatch III global model via ERDDAP dataset `NWW3_Global_Best`
  - variables: `Thgt` (significant wave height) → `swh_m`, `Tper` (peak period) → `peak_period_s`

See `src/config/sources.yaml` for human-readable source notes.

---

## Regions
Regions are defined in `src/config/regions.yaml` using a **bounding box**:

- `lat_min`, `lat_max` (degrees; south is negative)
- `lon_min`, `lon_max` (degrees; west is negative)

---

## BigQuery layout (Jakarta `asia-southeast2`)
Datasets:
- `ops` — operational tracking tables (runs, later: watermarks)
- `standard` — standardized driver tables

Tables:
- `ops.pipeline_runs` (one row per script execution)
- `standard.sst_daily`
- `standard.chl_8day`
- `standard.waves_daily`

---

## Repo structure (high level)
.
├── GLOSSARY.md
├── README.md
├── data
│   └── tmp
│       ├── chl_NTT_2024_01.nc
│       ├── sst_NTT_2024_01.nc
│       └── waves_NTT_2024_01.nc
├── requirements.txt
├── scripts
│   └── provision_bigquery.sh
├── sql
│   ├── create_features_tables.sql
│   ├── create_ops_tables.sql
│   ├── create_standard_tables.sql
│   └── verification
│       └── qa_checks_base_mart.sql
└── src
    ├── __init__.py
    ├── __pycache__
    │   └── __init__.cpython-314.pyc
    ├── config
    │   ├── regions.yaml
    │   └── sources.yaml
    └── ingest
        ├── __init__.py
        ├── __pycache__
        │   ├── __init__.cpython-314.pyc
        │   ├── chl.cpython-314.pyc
        │   ├── sst.cpython-314.pyc
        │   └── waves.cpython-314.pyc
        ├── chl.py
        ├── helpers
        │   ├── __init__.py
        │   ├── __pycache__
        │   │   ├── __init__.cpython-314.pyc
        │   │   ├── bigquery.cpython-314.pyc
        │   │   ├── bq_casting.cpython-314.pyc
        │   │   ├── cli_defaults.cpython-314.pyc
        │   │   ├── dates.cpython-314.pyc
        │   │   ├── df_validate.cpython-314.pyc
        │   │   ├── erddap.cpython-314.pyc
        │   │   ├── netcdf.cpython-314.pyc
        │   │   ├── pipeline.cpython-314.pyc
        │   │   ├── regions.cpython-314.pyc
        │   │   ├── syslogging.cpython-314.pyc
        │   │   └── xr_utils.cpython-314.pyc
        │   ├── bigquery.py
        │   ├── bq_casting.py
        │   ├── cli_defaults.py
        │   ├── dates.py
        │   ├── df_validate.py
        │   ├── erddap.py
        │   ├── netcdf.py
        │   ├── pipeline.py
        │   ├── regions.py
        │   ├── run_tracking.py
        │   ├── syslogging.py
        │   └── xr_utils.py
        ├── sst.py
        └── waves.py

---

## Data Layers
This project follows a layered data architecture in BigQuery.

### standard dataset — Standardized Physical Measurements
Contains cleaned, normalized, analysis-ready physical measurements per source.

Grain:
- sst_daily: one row per (region_id, date, lat, lon)
- waves_daily: one row per (region_id, date, lat, lon)
- chl_8day: one row per (region_id, period_start_date, period_end_date, lat, lon)

Characteristics:
- Units normalized (°C, meters, seconds, mg/m³)
- Fill values converted to NULL
- Partitioned by date (or period_start_date)
- Clustered by region_id
- No modeling logic

### features dataset — Model-Ready Tables
Contains region-aggregated, daily feature tables used for anomaly detection.

Grain:
- region_daily_base: one row per (region_id, date)
- region_daily_features: one row per (region_id, date) with lag/rolling features
- region_daily_base includes:
- sst_c_mean
- swh_m_mean
- peak_period_s_mean
- chl_mg_m3_mean (dailyized from 8-day windows)

region_daily_features adds:
- 1-day lags
- first differences
- 7-day rolling mean/std
- seasonal signals (day-of-year, month)

These tables are used directly for ML training and scoring.

### ops dataset — Pipeline Observability
Contains operational tracking tables.

ops.pipeline_runs:
- One row per job execution
- SUCCESS / FAILED
- rows_written
- start/end timestamps
- error snippet in notes

This enables:
- reproducibility
- debugging
- monitoring ingestion health

---

## Build Order
1) Create datasets (asia-southeast2 / Jakarta)
2) Run:
- sql/create_ops_tables.sql
- sql/create_standard_tables.sql
3) Run ingestion:
- python -m src.ingest.sst
- python -m src.ingest.chl
- python -m src.ingest.waves
4) Run:
- sql/create_features_tables.sql
5) Run QA:
- sql/qa_checks.sql
6)Train anomaly model

---

## BigQuery Location
All datasets are created in:
    asia-southeast2 (Jakarta)

All queries must be executed with:
    Processing location: asia-southeast2

BigQuery does not allow cross-region queries.

---

## Feature Engineering Philosophy
- Spatial signals are aggregated to region-level daily means.
- Chlorophyll 8-day composites are dailyized via window overlap.
- Rolling statistics capture short-term anomalies.
- No leakage from future dates is allowed in rolling windows.

---

## Setup

### 1) Prerequisites
- Python 3.10+ (recommended)
- Google Cloud SDK (`gcloud`, `bq`)
- A GCP project with permissions to create BigQuery datasets/tables

### 2) Create and activate venv
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

### 3) Authenticate to GCP (ADC for Python)
```bash
gcloud auth login
gcloud config set project <YOUR_GCP_PROJECT_ID>
gcloud auth application-default login
gcloud auth application-default set-quota-project <YOUR_GCP_PROJECT_ID>

### 4) Provision BigQuery (Jakarta: asia-southeast2)
Creates datasets ops and standard, and all required tables.

```bash
./scripts/provision_bigquery.sh <YOUR_GCP_PROJECT_ID>

Verify:

```bash
bq ls <YOUR_GCP_PROJECT_ID>:standard
bq ls <YOUR_GCP_PROJECT_ID>:ops

---

## Ingestion (module execution)

### SST (NOAA OISST daily)
Dry run (download + parse only; still logs to ops.pipeline_runs):

```bash
python -m src.ingest.sst \
  --region_id NTT \
  --year 2001 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --dry_run


Load to BigQuery (idempotent monthly reload):

```bash
python -m src.ingest.sst \
  --region_id NTT \
  --year 2001 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --replace


### Chlorophyll-a (8-day composites)
Dry run:

```bash
python -m src.ingest.chl \
  --region_id NTT \
  --year 2024 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --dry_run


Load:

```bash
python -m src.ingest.chl \
  --region_id NTT \
  --year 2024 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --replace


### Waves (WW3 hourly → daily mean)
Dry run:

```bash
python -m src.ingest.waves \
  --region_id NTT \
  --year 2024 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --dry_run


Load:
```bash
python -m src.ingest.waves \
  --region_id NTT \
  --year 2024 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --replace

---

## Validation queries (BigQuery)

### SST
```sql
SELECT region_id, COUNT(*) rows, MIN(date) min_date, MAX(date) max_date
FROM `standard.sst_daily`
WHERE region_id='NTT' AND date BETWEEN '2001-01-01' AND '2001-01-31'
GROUP BY region_id;

### Chlorophyll
```sql
SELECT region_id, COUNT(*) rows, MIN(period_start_date) min_start, MAX(period_end_date) max_end
FROM `standard.chl_8day`
WHERE region_id='NTT'
  AND period_start_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY region_id;

### Waves
```sql
SELECT region_id, COUNT(*) rows, MIN(date) min_date, MAX(date) max_date
FROM `standard.waves_daily`
WHERE region_id='NTT' AND date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY region_id;

---

## Run logging (ops.pipeline_runs)
Each ingestion execution writes exactly one row to:
<PROJECT>.ops.pipeline_runs with:
    - run_id, job_name, start_ts, end_ts, status
    - rows_written (0 on dry_run or failure)
    - notes (parameters + dataset_id + error snippet on failure)

This gives a simple audit trail and makes it easy to build dashboards/alerts later.

---

## Notes on cost

BigQuery costs are mainly query scans + storage.
Tables are partitioned by date and clustered by region_id to reduce scan cost.
Ingestion runs are typically light (subset by bbox + month), and NetCDF downloads are cached locally under data/tmp/.
