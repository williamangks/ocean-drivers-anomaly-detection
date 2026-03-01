This document defines the schema, grain, units, and semantics of all production tables in this project.

All BigQuery datasets are located in:
    asia-southeast2 (Jakarta)

---

# ops.pipeline_runs

Purpose:
Operational monitoring and run tracking.

Grain:
One row per pipeline execution.

Partition:
DATE(start_ts)

Columns:
- run_id (STRING)  
  Unique UUID for the pipeline execution.

- job_name (STRING)  
  Logical job name (e.g. ingest_sst, ingest_chl, ingest_waves).

- start_ts (TIMESTAMP)  
  UTC timestamp when job started.

- end_ts (TIMESTAMP)  
  UTC timestamp when job finished.

- status (STRING)  
  SUCCESS or FAILED.

- rows_written (INT64)  
  Number of rows written to destination table.

- notes (STRING)  
  Free-text metadata (parameters, error snippet).

---

# standard.sst_daily

Source:
NOAA OISST v2.1 via ERDDAP (dataset: ncdcOisst21Agg)

Purpose:
Daily sea surface temperature grid data.

Grain:
(region_id, date, lat, lon)

Partition:
date

Cluster:
region_id

Columns:

- date (DATE)  
  Observation day (UTC).

- region_id (STRING)  
  Region key from regions.yaml.

- lat (FLOAT64)  
  Latitude (WGS84).

- lon (FLOAT64)  
  Longitude (WGS84; converted from 0–360 if needed).

- sst_c (FLOAT64)  
  Sea Surface Temperature in degrees Celsius.

- source (STRING)  
  Data provenance identifier.

- ingested_at (TIMESTAMP)  
  Ingestion timestamp (UTC).

Null Policy:
- sst_c may be NULL due to missing data.
- All other columns are REQUIRED.

---

# standard.chl_8day

Source:
NOAA CoastWatch via ERDDAP (dataset: erdMBchla8day_LonPM180)

Purpose:
8-day chlorophyll-a composite grid data.

Grain:
(region_id, period_start_date, period_end_date, lat, lon)

Partition:
period_start_date

Cluster:
region_id

Columns:
- period_start_date (DATE)  
  Start of composite window (UTC).

- period_end_date (DATE)  
  End of composite window (UTC).

- region_id (STRING)  
  Region key.

- lat (FLOAT64)  
  Latitude (WGS84).

- lon (FLOAT64)  
  Longitude (-180 to 180).

- chl_mg_m3 (FLOAT64)  
  Chlorophyll-a concentration (mg/m³).

- source (STRING)  
  Data provenance identifier.

- ingested_at (TIMESTAMP)  
  Ingestion timestamp (UTC).

Time Handling:
ERDDAP provides centered timestamps. Window is derived as:

period_start_date = center_date - 3 days  
period_end_date   = center_date + 4 days

Null Policy:
High NULL fraction possible due to cloud coverage and masking.

---

# standard.waves_daily

Source:
WaveWatch III Global Model via ERDDAP (dataset: NWW3_Global_Best)

Purpose:
Daily-aggregated wave model data.

Grain:
(region_id, date, lat, lon)

Partition:
date

Cluster:
region_id

Columns:
- date (DATE)  
  Observation day (UTC).

- region_id (STRING)  
  Region key.

- lat (FLOAT64)  
  Latitude (WGS84).

- lon (FLOAT64)  
  Longitude (0–360 converted as needed).

- swh_m (FLOAT64)  
  Significant Wave Height (meters).

- peak_period_s (FLOAT64)  
  Peak Wave Period (seconds).

- source (STRING)  
  Data provenance identifier.

- ingested_at (TIMESTAMP)  
  Ingestion timestamp (UTC).

Processing Notes:
- Hourly model output aggregated to daily mean.
- Dateline-crossing regions handled via longitude split.
- Physical range filters applied before aggregation.

---

#features.region_daily_base

Purpose:
Region-level daily aggregated physical signals.

Grain:
(region_id, date)

Derived From:
standard.sst_daily  
standard.waves_daily  
standard.chl_8day

Columns:
- date (DATE)  
  UTC day.

- region_id (STRING)  
  Region key.

- sst_c_mean (FLOAT64)  
  Spatial mean SST for the region.

- swh_m_mean (FLOAT64)  
  Spatial mean significant wave height.

- peak_period_s_mean (FLOAT64)  
  Spatial mean peak wave period.

- chl_mg_m3_mean (FLOAT64)  
  Dailyized spatial mean chlorophyll.

Chlorophyll Dailyization:
1. Compute spatial mean per 8-day window.
2. Expand window to daily dates using:
   GENERATE_DATE_ARRAY(period_start_date, period_end_date)
3. Assign window mean to each overlapping day.

---

# features.region_daily_features

Purpose:
Model-ready feature table for anomaly detection.

Grain:
(region_id, date)

Derived From:
features.region_daily_base

Raw Signals:
- sst_c_mean
- swh_m_mean
- peak_period_s_mean
- chl_mg_m3_mean

Lag Features (1 day):
- sst_lag1
- swh_lag1
- peak_period_lag1
- chl_lag1

First Differences:
- sst_diff1
- swh_diff1
- peak_period_diff1
- chl_diff1

Rolling 7-Day Window:
Defined as:
ROWS BETWEEN 6 PRECEDING AND CURRENT ROW

Rolling Features:
- sst_ma7
- sst_sd7
- swh_ma7
- swh_sd7
- peak_period_ma7
- peak_period_sd7
- chl_ma7
- chl_sd7

Time Signals:
- doy (Day of Year)
- month (Calendar month)

---

# Units Summary
- sst_c: degrees Celsius (°C)
- swh_m: meters
- peak_period_s: seconds
- chl_mg_m3: mg/m³

---
# Spatial Reference
Coordinate system: WGS84  
Latitude: South negative, North positive  
Longitude: Consistent regional domain

---

# Modeling Intent
The final feature table is designed for:
- Isolation Forest
- Density-based anomaly detection
- Robust covariance methods
- Change-point detection

Each row represents one region-day observation in multivariate feature space.
