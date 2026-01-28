# Glossary

This glossary defines key acronyms and technical terms used in this project.  
It is intended to make the repository accessible to both data engineers and non-oceanography readers.

---

## Core Ocean & Climate Terms

### **NOAA**
**National Oceanic and Atmospheric Administration** (United States).  
A major provider of open ocean and climate datasets.

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

### **Waves (Wave Reanalysis)**
Wave parameters such as significant wave height are used as proxies for ocean energy and surface mixing.

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
A NOAA-hosted data server that allows users to query and download subsets of scientific datasets over HTTP.

---

### **NetCDF**
**Network Common Data Form** — a standard file format for storing multi-dimensional climate and ocean data (e.g., time × lat × lon grids).

---

## Data Engineering Terms

### **Backfill**
Batch processing of historical data over a fixed time range (e.g., 2001–2020).  
Backfills are common when building long-term baselines before incremental pipelines.

---

### **boundbox (Bounding Box)**
A geographic rectangle defined by:

- `lat_min`, `lat_max`
- `lon_min`, `lon_max`

Used to subset global gridded datasets to a specific Region of Interest (ROI).

---

### **Standard Layer (`standard`)**
A warehouse layer containing cleaned and standardized physical measurements.

Characteristics:
- consistent schema across sources
- normalized units
- aligned spatial structure
- no ML feature engineering yet

Example tables:
- `standard.sst_daily`
- `standard.chl_8day`
- `standard.waves_daily`

---

### **Feature Layer (`features`)**
A modeling-ready layer derived from the standard layer.

Includes:
- rolling statistics
- climatology anomalies
- lag features

---

### **Mart Layer (`mart`)**
A consumer-facing analytics layer designed for dashboards and reporting.

Optimized for:
- Looker Studio queries
- maps and regional summaries

---

## Machine Learning Terms

### **Anomaly Detection**
The task of identifying observations that deviate significantly from normal patterns.

In this project, anomalies represent unusual environmental conditions in ocean drivers.

---

### **Isolation Forest**
An unsupervised anomaly detection algorithm that identifies anomalies by isolating rare observations using random decision trees.

Chosen because it:
- scales well to large datasets
- works in high-dimensional feature spaces
- does not require labeled anomalies

---

## Project-Specific Concepts

### **Environmental Drivers**
Variables that describe ocean conditions and may influence ecosystem changes.

Drivers used here:
- SST (temperature)
- Chlorophyll-a (biological proxy)
- Wave energy (physical forcing)

---

### **Transferability Validation**
Testing whether anomaly patterns learned in one region (e.g., Hawaiʻi) generalize to another region (e.g., NTT, Indonesia).

---

