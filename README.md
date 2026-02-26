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
tree .



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
