-- sst
SELECT
  region_id,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT date) AS n_days,

  SAFE_DIVIDE(COUNTIF(sst_c IS NULL), COUNT(*)) AS frac_null_sst,
  SAFE_DIVIDE(COUNTIF(sst_c < 0 OR sst_c > 40), COUNT(*)) AS frac_out_of_range_sst

FROM `de-anomaly-detection-project.standard.sst_daily`
WHERE region_id = 'NTT'
GROUP BY region_id;

-- waves
SELECT
  region_id,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT date) AS n_days,

  SAFE_DIVIDE(COUNTIF(swh_m IS NULL), COUNT(*)) AS frac_null_swh,
  SAFE_DIVIDE(COUNTIF(peak_period_s IS NULL), COUNT(*)) AS frac_null_peak_period

FROM `de-anomaly-detection-project.standard.waves_daily`
WHERE region_id = 'NTT'
GROUP BY region_id;

-- chl
SELECT
  region_id,
  MIN(period_start_date) AS min_start,
  MAX(period_end_date) AS max_end,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT CONCAT(CAST(period_start_date AS STRING), ':', CAST(period_end_date AS STRING))) AS n_windows,

  SAFE_DIVIDE(COUNTIF(chl_mg_m3 IS NULL), COUNT(*)) AS frac_null_chl

FROM `de-anomaly-detection-project.standard.chl_8day`
WHERE region_id = 'NTT'
GROUP BY region_id;
