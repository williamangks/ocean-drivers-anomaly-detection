-- create_standard_tables.sql
-- Purpose:
--   Create "standard" tables: standardized, analysis-ready tables per data source.
--   These tables normalize units, schema, and spatial/temporal structure
--   before feature engineering and modeling.
--
-- Layer definition:
--   standard = cleaned, standardized physical measurements
--   (no anomaly logic, no ML features yet)

--------------------------------------------------------------------------------
-- 1) Sea Surface Temperature (SST) — daily
-- Source: NOAA OISST (Optimum Interpolation Sea Surface Temperature)
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS standard.sst_daily (
  date DATE NOT NULL,
  region_id STRING NOT NULL,  -- example 'HAWAII', 'NTT'
  lat FLOAT64 NOT NULL,
  lon FLOAT64 NOT NULL,
  sst_c FLOAT64,              -- Sea Surface Temperature (degrees Celsius)
  source STRING NOT NULL,              -- example 'NOAA_OISST'
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY region_id;

--------------------------------------------------------------------------------
-- 2) Chlorophyll-a (8-day composite)
-- Source: NOAA CoastWatch ERDDAP
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS standard.chl_8day (
  period_start_date DATE NOT NULL,
  period_end_date DATE NOT NULL,
  region_id STRING NOT NULL,
  lat FLOAT64 NOT NULL,
  lon FLOAT64 NOT NULL,
  chl_mg_m3 FLOAT64,          -- Chlorophyll-a concentration (mg/m^3)
  source STRING NOT NULL,              -- example 'NOAA_ERDDAP_CHLA_8DAY'
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY period_start_date
CLUSTER BY region_id;

--------------------------------------------------------------------------------
-- 3) Waves — daily aggregated
-- Source: Copernicus Marine Global Ocean Waves Reanalysis
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS standard.waves_daily (
  date DATE NOT NULL,
  region_id STRING NOT NULL,
  lat FLOAT64 NOT NULL,
  lon FLOAT64 NOT NULL,
  swh_m FLOAT64,              -- Significant Wave Height (meters)
  peak_period_s FLOAT64,      -- Peak wave period (seconds)
  source STRING NOT NULL,              -- example 'CMEMS_WAVES_REANALYSIS'
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY region_id;
