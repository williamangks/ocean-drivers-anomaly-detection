# Ocean Environmental Anomaly Platform

End-to-end data platform for detecting and interpreting **multivariate ocean environmental anomalies** using long-term historical data (2001–2020).  
This project focuses on **batch/backfill** processing and reproducible analytics outputs for dashboards.

## Why this project
Ocean systems can shift in abnormal ways due to changes in multiple environmental drivers (e.g., temperature, chlorophyll, waves).  
This repository builds a pipeline to:

- ingest and standardize gridded ocean datasets for two regions (**Hawaiʻi** and **NTT, Indonesia**)
- engineer anomaly-friendly features
- detect multivariate anomalies using **Isolation Forest**
- interpret relationships between drivers (co-occurrence and lead/lag) *conditional on anomalous periods*
- publish results to **BigQuery** for visualization in **Looker Studio**

## Scope
### Included
- Backfill (historical) pipeline for **2001–2020**
- Regions:
  - **HAWAII** (Hawaiian Islands bounding box)
  - **NTT** (East Nusa Tenggara bounding box)
- Outputs for:
  - **Map drill-down** (geopoint anomaly view)
  - **Regional interpretation** (time-series metrics)
- Monitoring via:
  - **Cloud Logging** (job logs)
  - **Cloud Monitoring** (Cloud Run Job metrics + alerts)

### Not included (by design)
- True incremental scheduling (this is handled in a separate Project 2 repo)
- Public REST prediction API for dashboard serving
- Prometheus/Grafana stack

## Data Sources (high level)
This project uses global ocean datasets and subsets them by region.

- **NOAA OISST** (Sea Surface Temperature, daily)  
  NOAA = National Oceanic and Atmospheric Administration (US)  
  OISST = Optimum Interpolation Sea Surface Temperature

- **NOAA CoastWatch ERDDAP** (Chlorophyll-a, 8-day composites)  
  ERDDAP = Environmental Research Division’s Data Access Program (data server)

- **Copernicus Marine** (Global waves reanalysis; aggregated to match analysis grain)  
  Reanalysis = reconstruction using models + observations

See `src/config/sources.yaml` for the documented list and roles.

## Architecture (GCP)
**Storage & serving**
- **GCS (Cloud Storage)**: raw/curated artifacts + model versions
- **BigQuery**: curated tables + mart tables (Looker Studio reads from here)
- **Looker Studio**: dashboards (map + interpretation)

**Compute**
- Local dev / notebook prototyping (optional)
- Batch execution via scripts (and optionally Cloud Run Jobs later)

**Observability**
- **Cloud Logging**: job logs, error traces
- **Cloud Monitoring**: basic job metrics + alerting on failures

## Model artifact storage (no API)
Models are stored as versioned artifacts in **GCS**, not served by an API.

Example layout:
- `gs://<YOUR_BUCKET>/project1/models/iforest/runs/<RUN_ID>/model.joblib`
- `.../feature_schema.json` (ordered feature list to prevent column-order bugs)
- `.../train_config.json`
- `.../train_watermark.json`
- `.../metrics.json`
- `gs://<YOUR_BUCKET>/project1/models/iforest/LATEST.json` (pointer to active version)

This keeps the system simple, cheap, and reproducible.

## Configuration
Regions are defined in `src/config/regions.yaml` using a **boundbox** (bounding box):

- `lat_min`, `lat_max` (degrees)
- `lon_min`, `lon_max` (degrees)

> Note: Latitude south is negative; longitude west is negative.

## BigQuery datasets (recommended)
Create these datasets in your project:
- `ops` — run tracking tables
- `curated` — standardized driver tables and feature tables
- `mart` — dashboard-ready tables

SQL starter: `sql/create_ops_tables.sql`

## Repository layout
src/
config/ # regions.yaml, sources.yaml
ingest/ # fetch/subset each driver
transform/ # standardize + feature engineering
model/ # train + score isolation forest
mart/ # build dashboard tables
sql/ # BigQuery DDL
.github/workflows/ # optional automation later



## Outputs for Looker Studio
This project produces two primary dashboard tables:

1) **Geopoint drill-down**
- anomaly points by time and location for interactive map exploration

2) **Regional interpretation**
- daily/weekly regional metrics (anomaly rate, average score, driver summaries)

## Disclaimer (important)
This project performs **anomaly detection and association analysis**.  
It does **not** claim causal attribution (e.g., “X causes Y”). Interpretations are framed as:
- co-occurrence patterns
- lead/lag relationships
- conditional relationships during anomalous windows

## Getting started (high level)
1) Configure regions and sources:
- `src/config/regions.yaml`
- `src/config/sources.yaml`

2) Create BigQuery ops table:
- run `sql/create_ops_tables.sql`

3) Backfill drivers (2001–2020):
- ingest SST → ingest chlorophyll → ingest waves

4) Build features → train model → score → publish mart tables

---

### Project metadata
- GCP project: `<YOUR_GCP_PROJECT_ID>`
- GCS bucket: `gs://<YOUR_BUCKET>`
- Regions: Hawaiʻi, NTT (Indonesia)
- Period: 2001–2020

---

## Provision BigQuery (Jakarta region)
This project uses BigQuery datasets created in `asia-southeast2` (Jakarta).

```bash
./scripts/provision_bigquery.sh <YOUR_GCP_PROJECT_ID>

---
