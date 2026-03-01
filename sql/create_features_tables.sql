CREATE SCHEMA IF NOT EXISTS `de-anomaly-detection-project.features`
OPTIONS (location = "asia-southeast2");

CREATE SCHEMA IF NOT EXISTS `de-anomaly-detection-project.mart`
OPTIONS (location = "asia-southeast2");


CREATE OR REPLACE TABLE `de-anomaly-detection-project.features.region_daily_base` AS
WITH
-- SST: daily region mean
sst AS (
  SELECT
    date,
    region_id,
    AVG(sst_c) AS sst_c_mean
  FROM `de-anomaly-detection-project.standard.sst_daily`
  WHERE region_id = 'NTT'
  GROUP BY date, region_id
),

-- Waves: daily region mean
waves AS (
  SELECT
    date,
    region_id,
    AVG(swh_m) AS swh_m_mean,
    AVG(peak_period_s) AS peak_period_s_mean
  FROM `de-anomaly-detection-project.standard.waves_daily`
  WHERE region_id = 'NTT'
  GROUP BY date, region_id
),

-- Chlorophyll: do NOT explode to daily grid rows.
-- Instead: aggregate per 8-day window first (spatial mean), then map to days.
chl_window_mean AS (
  SELECT
    region_id,
    period_start_date,
    period_end_date,
    AVG(chl_mg_m3) AS chl_mg_m3_window_mean
  FROM `de-anomaly-detection-project.standard.chl_8day`
  WHERE region_id = 'NTT'
  GROUP BY region_id, period_start_date, period_end_date
),

-- Dailyize *windows only* (34 windows -> ~272 days max), not all grid points
chl_daily AS (
  SELECT
    d AS date,
    region_id,
    -- if multiple windows overlap a day (rare), average them
    AVG(chl_mg_m3_window_mean) AS chl_mg_m3_mean
  FROM chl_window_mean,
  UNNEST(GENERATE_DATE_ARRAY(period_start_date, period_end_date)) AS d
  GROUP BY date, region_id
),

cal AS (
  SELECT DISTINCT date, region_id FROM sst
  UNION DISTINCT SELECT DISTINCT date, region_id FROM waves
  UNION DISTINCT SELECT DISTINCT date, region_id FROM chl_daily
)

SELECT
  cal.date,
  cal.region_id,
  sst.sst_c_mean,
  waves.swh_m_mean,
  waves.peak_period_s_mean,
  chl_daily.chl_mg_m3_mean
FROM cal
LEFT JOIN sst USING (date, region_id)
LEFT JOIN waves USING (date, region_id)
LEFT JOIN chl_daily USING (date, region_id);

CREATE OR REPLACE TABLE `de-anomaly-detection-project.features.region_daily_features` AS
WITH base AS (
  SELECT * FROM `de-anomaly-detection-project.features.region_daily_base`
),

feat AS (
  SELECT
    date,
    region_id,

    sst_c_mean,
    swh_m_mean,
    peak_period_s_mean,
    chl_mg_m3_mean,

    -- lags
    LAG(sst_c_mean) OVER w AS sst_lag1,
    LAG(swh_m_mean) OVER w AS swh_lag1,
    LAG(peak_period_s_mean) OVER w AS peak_period_lag1,
    LAG(chl_mg_m3_mean) OVER w AS chl_lag1,

    -- diffs
    (sst_c_mean - LAG(sst_c_mean) OVER w) AS sst_diff1,
    (swh_m_mean - LAG(swh_m_mean) OVER w) AS swh_diff1,
    (peak_period_s_mean - LAG(peak_period_s_mean) OVER w) AS peak_period_diff1,
    (chl_mg_m3_mean - LAG(chl_mg_m3_mean) OVER w) AS chl_diff1,

    -- rolling 7d
    AVG(sst_c_mean) OVER w7 AS sst_ma7,
    STDDEV_SAMP(sst_c_mean) OVER w7 AS sst_sd7,

    AVG(swh_m_mean) OVER w7 AS swh_ma7,
    STDDEV_SAMP(swh_m_mean) OVER w7 AS swh_sd7,

    AVG(peak_period_s_mean) OVER w7 AS peak_period_ma7,
    STDDEV_SAMP(peak_period_s_mean) OVER w7 AS peak_period_sd7,

    AVG(chl_mg_m3_mean) OVER w7 AS chl_ma7,
    STDDEV_SAMP(chl_mg_m3_mean) OVER w7 AS chl_sd7,

    -- time signals
    EXTRACT(DAYOFYEAR FROM date) AS doy,
    EXTRACT(MONTH FROM date) AS month
  FROM base
  WINDOW
    w AS (PARTITION BY region_id ORDER BY date),
    w7 AS (PARTITION BY region_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT * FROM feat;
