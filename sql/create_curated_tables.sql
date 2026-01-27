-- create_curated_tables.sql usage:
--   Create "curated" tables: standardized, analysis-ready tables per data source.
--   These tables are the cleaned/normalized layer that downstream feature engineering joins on.
--
-- Design choices:
--		- region_id: named region from src/config/regions.yaml (uses "boundbox" to subset data)
--		- lat/lon: grid cell center coordinates (float)
--		- Partition by date to keep queries efficient
--		- Cluster by region_id to reduce scan cost for region-filtered queries

CREATE SCHEMA IF NOT EXISTS curated;

------
-- 1) Sea Surface Temperature (SST) - daily
-- Source: NOAA OISST (Optimum Interpolation Sea Surface Temperature)
--
-- Grain: One row per (date, region_id, lat, lon)
------
CREATE TABLE IF NOT EXISTS curated.sst_daily (
	date DATE NOT NULL,
	region_id STRING NOT NULL,	-- 'HAWAII', 'NTT'
	lat FLOAT64 NOT NULL,
	lon FLOAT64 NOT NULL,
	sst_c FLOAT64,				-- SST in degrees - Celsius
	source STRING,				-- 'NOAA_OISST', 'ERDDAP', etc.
	ingested_at TIMESTAMP		-- the time this row was loaded
)
PARTITION BY date
CLUSTER BY region_id;

-----
-- 2) Chlorophyll-a (Chl-a) - 8day composite
-- Source: NOAA CoastWatch ERDDAP (Environmental Research Division's Data Access Program)
--
-- Grain: One row per (period_start_date, period_end_date, region_id, lat, lon)
-- Notes:
--		- Many ocean color products are provided as composited periods (8-day) due to cloud cover.
--		- Store both start/end so you can align to daily later (e.g., forward-fill to each day in window)
-----
CREATE TABLE IF NOT EXISTS curated.chl_8day (
	period_start_date DATE NOT NULL,
	period_end_date DATE NOT NULL,
	region_id STRING NOT NULL,
	lat FLOAT64 NOT NULL,
	lon FLOAT64 NOT NULL,
	chl_mg_m3 FLOAT64,					-- chlorophyll-a concentration (typically mg/m^3)
	source STRING,						-- 'NOAA_ERDDAP_MB_CHLA_8DAY'
	ingested_at TIMESTAMP
)
PARTITION BY period_start_date
CLUSTER BY region_id;

-----
-- 3) Waves - aggregated (daily)
-- Source: Copernicus Marine Global Ocean Waves Reanalysis
--
-- The source itself can provide the raw waves data 3-hourly.
-- But to keep the platform cheap and BI-friendly, we store daily aggregates.
--
-- Grain: One row per (date, region_id, lat, lon)
--
-- Suggested variables:
--		- swh_m: Significant Wave Height (meters)
--		- peak_period_s: Peak wave period (seconds)
-----
CREATE TABLE IF NOT EXISTS curated.waves_daily (
	date DATE NOT NULL,
	region_id STRING NOT NULL,
	lat FLOAT64 NOT NULL,
	lon FLOAT64 NOT NULL,
	swh_m FLOAT64,				-- significant wave height
	peak_period_s FLOAT64,		-- peak wave period
	source STRING,				-- 'CMEMS_WAVES_REANALYSIS'
	ingested_at TIMESTAMP
)
PARTITION BY date
CLUSTER BY region_id;

-----
-- 4) "grid registry" per region
-- Helps ensure consistent lat/lon grid cells across sources after subsetting.
-- The data maybe  useful later if grids differ.
-----
CREATE TABLE IF NOT EXISTS curated.grid_points (
  region_id STRING NOT NULL,
  lat FLOAT64 NOT NULL,
  lon FLOAT64 NOT NULL,
  created_at TIMESTAMP
)
CLUSTER BY region_id;
