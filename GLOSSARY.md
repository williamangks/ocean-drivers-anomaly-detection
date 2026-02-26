# Glossary

This glossary defines key acronyms and technical terms used in this project.  
It is intended to make the repository accessible to both data engineers and non-oceanography readers.

---

## Core Ocean & Climate Terms

### **NOAA**
**National Oceanic and Atmospheric Administration** (United States).  
A major provider of open ocean and climate datasets and ERDDAP endpoints.

---

### **SST**
**Sea Surface Temperature** — the temperature of the upper layer of the ocean.  
SST is a key environmental driver influencing marine ecosystems and climate patterns.

---

### **OISST**
**Optimum Interpolation Sea Surface Temperature** (NOAA product).  
A daily, gap-filled SST dataset commonly used for anomaly detection and climate monitoring.

---

### **Chlorophyll-a (Chl-a)**
A satellite-derived proxy for phytoplankton biomass in the ocean.  
Often used as an indicator of biological productivity.

---

### **Waves**
Ocean surface wave conditions. In this repo, waves are represented using:
- **SWH** (Significant Wave Height) — meters
- **Peak Period** — seconds

Used as proxies for ocean surface energy, mixing, and exposure conditions.

---

### **Reanalysis**
A scientifically consistent reconstruction of historical environmental conditions using both:
- observations (satellites, buoys)
- numerical models

Reanalysis datasets provide long-term coverage even where direct measurements are incomplete.

---

## Data Access & Formats

### **ERDDAP**
**Environmental Research Division’s Data Access Program**.  
A data server (commonly NOAA-hosted) that lets you query and download subsets of scientific datasets over HTTP.

This project uses **ERDDAP griddap** to download NetCDF subsets (time × lat × lon) by region bounding box.

---

### **griddap**
An ERDDAP API endpoint for **gridded** datasets.  
It supports slicing by:
- time ranges
- latitude/longitude bounds
- (sometimes) a singleton dimension like depth or altitude

---

### **NetCDF**
**Network Common Data Form** — a standard file format for storing multi-dimensional climate and ocean data (e.g., time × lat × lon grids).

In this repo, NetCDF files are cached locally under `data/tmp/` to avoid re-downloading.

---

### **Bounding Box (BBox)**
A geographic rectangle defined by:

- `lat_min`, `lat_max`
- `lon_min`, `lon_max`

Used to subset global gridded datasets to a specific Region of Interest (ROI).

---

### **Dateline Split**
A special case when a bounding box crosses the International Date Line.  
Some datasets represent longitude as **0..360** instead of **-180..180**, and a bbox may need to be split into two longitude intervals:
- `[a..360]` and `[0..b]`

This repo’s **waves ingestion** supports this by making up to two ERDDAP requests.

---

## Dataset / Project-Specific Terms

### **WW3 / WaveWatch III**
A global wave model used to represent ocean wave conditions.  
In this repo, WaveWatch III data is pulled via ERDDAP dataset `NWW3_Global_Best`.

---

### **8-day Composite**
A satellite product that aggregates multiple daily observations into a single value representing a time window (here: 8 days).  
ERDDAP often exposes a single “center” timestamp for each composite; this repo converts it to an 8-day window:
- `period_start_date = center - 3 days`
- `period_end_date   = center + 4 days`

---

### **Standard Layer (`standard`)**
A warehouse layer containing cleaned and standardized physical measurements.

Characteristics:
- consistent schema across sources
- normalized units where possible
- aligned spatial structure (lat/lon grids)
- no ML feature engineering yet

Example tables:
- `standard.sst_daily`
- `standard.chl_8day`
- `standard.waves_daily`

---

### **Ops Layer (`ops`)**
A warehouse layer containing operational/observability tables.

In this repo:
- `ops.pipeline_runs`: one row per ingestion execution (SUCCESS/FAILED, timestamps, rows_written, notes)

This is used for auditing, debugging failures, and building simple monitoring later.

---

### **Idempotent Run**
A run that can be safely re-executed without producing duplicates or inconsistent outputs.

In this repo, idempotency is achieved using `--replace`, which deletes existing rows for the region+time window before loading.

---

### **Dry Run**
Runs the full pipeline (download → parse → transform → validate) but **skips loading into BigQuery**.  
This is useful for debugging parsing, schema, and row counts.

Note: the run may still be logged to `ops.pipeline_runs` (depending on script implementation).

---

### **Backfill**
Batch processing of historical data over a fixed time range (e.g., 2001–2020).  
Backfills are common when building long-term baselines before incremental pipelines.

---

## Machine Learning Terms (planned phase)

### **Anomaly Detection**
The task of identifying observations that deviate significantly from normal patterns.

In this project, anomalies represent unusual environmental conditions in ocean drivers.

---

### **Isolation Forest**
An unsupervised anomaly detection algorithm that identifies anomalies by isolating rare observations using random decision trees.

Often chosen because it:
- scales well to large datasets
- works in high-dimensional feature spaces
- does not require labeled anomalies

---

### **Transferability Validation**
Testing whether anomaly patterns learned in one region (e.g., Hawaiʻi) generalize to another region (e.g., NTT, Indonesia).
