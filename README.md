# Ocean Drivers Data Platform (SST + Chlorophyll)

A practical data engineering project that ingests and standardizes **ocean environmental driver** datasets for two regions (**HAWAII** and **NTT, Indonesia**) into **BigQuery (Jakarta region: asia-southeast2)**.

The repo is built to be:
- reproducible (provision + run from scratch)
- cheap (free-tier friendly)
- production-like (idempotent runs, retries, caching, validation, logging)

---

## What this project is (current status)
Implemented now:
- BigQuery provisioning (datasets + tables) via script
- SST ingestion (NOAA OISST v2.1 via ERDDAP) → `standard.sst_daily`
- Chlorophyll ingestion (8-day composites via ERDDAP) → `standard.chl_8day`
- Shared ingestion helpers (download retry, atomic writes, NetCDF validation, schema checks)

Planned next:
- Waves ingestion → `standard.waves_daily`
- Pipeline run tracking + per-source watermarks
- Weekly orchestration (Cloud Scheduler → Cloud Run Jobs or GitHub Actions cron)

---

## Data sources (high level)
This project subsets global gridded datasets by region bounding box.

- **SST (daily):** NOAA OISST v2.1 via ERDDAP  
- **Chlorophyll-a (8-day):** NOAA CoastWatch ERDDAP dataset `erdMBchla8day_LonPM180`
- **Waves:** TBD (will choose an open dataset with no auth if possible)

See `src/config/sources.yaml` for human-readable source notes.

---

## Regions
Regions are defined in `src/config/regions.yaml` using a **boundbox** (bounding box):

- `lat_min`, `lat_max` (degrees)
- `lon_min`, `lon_max` (degrees)

Notes:
- latitude south is negative
- longitude west is negative

---

## BigQuery layout
Datasets (Jakarta region `asia-southeast2`):
- `ops` — operational tracking tables (runs, later: watermarks)
- `standard` — standardized driver tables

Tables:
- `standard.sst_daily`
- `standard.chl_8day`
- `standard.waves_daily` (placeholder for next ingestion)

---

## Repo structure
src/
|_ config/
|    |_ regions.yaml
|    |_ sources.yaml
|
|_ ingest/
|    |_ sst.py
|    |_ chl.py
|
|_ helpers/
|    |_ bigquery.py
|    |_ dates.py
|    |_ df_validate.py
|    |_ erddap.py
|    |_ netcdf.py
|    |_ regions.py
|    |_ syslogging.py
|    |_ xr_utils.py
|
|_ sql/
|    |_ create_ops_tables.sql
|    |_ create_standard_tables.sql
|
|_ scripts/
|    |_ provision_bigquery.sh





---

## Setup from scratch

### 1) Prerequisites
- Python 3.10+ recommended
- Google Cloud SDK (`gcloud`, `bq`)
- Access to a GCP project where you can create BigQuery datasets

### 2) Create and activate venv
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


### 3) Authenticate to GCP (ADC for Python)
gcloud auth login
gcloud config set project <YOUR_GCP_PROJECT_ID>
gcloud auth application-default login
gcloud auth application-default set-quota-project <YOUR_GCP_PROJECT_ID>


### 4) Provision BigQuery (Jakarta: asia-southeast2)
This creates datasets ops and standard and creates all required tables.
./scripts/provision_bigquery.sh <YOUR_GCP_PROJECT_ID>

Verify:
bq ls <YOUR_GCP_PROJECT_ID>:standard


---
## Ingestion
### SST (NOAA OISST daily)
Dry run (download + parse only):
python -m src.ingest.sst \
  --region_id NTT \
  --year 2001 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --dry_run

Load to BigQuery:
python -m src.ingest.sst \
  --region_id NTT \
  --year 2001 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --replace

### Chlorophyll-a (8-day composites)
Dry run:
python -m src.ingest.chl \
  --region_id NTT \
  --year 2024 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --dry_run

Load to BigQuery:
python -m src.ingest.chl \
  --region_id NTT \
  --year 2024 \
  --month 1 \
  --bq_project <YOUR_GCP_PROJECT_ID> \
  --replace


---
## Validation queries (BigQuery)
### SST:
SELECT region_id, COUNT(*) rows, MIN(date) min_date, MAX(date) max_date
FROM `standard.sst_daily`
WHERE region_id='NTT' AND date BETWEEN '2001-01-01' AND '2001-01-31'
GROUP BY region_id;

### Chlorophyll:
SELECT region_id, COUNT(*) rows, MIN(period_start_date) min_start, MAX(period_end_date) max_end
FROM `standard.chl_8day`
WHERE region_id='NTT' AND period_start_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY region_id;


---
## Notes on Cost
BigQuery costs are mostly driven by query scans and storage. This project keeps tables partitioned by date and clustered by region_id to reduce scan cost.
