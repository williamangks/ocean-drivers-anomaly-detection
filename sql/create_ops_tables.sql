-- create_ops_tables.sql usage:
--	Creates operational (ops) tables in BigQuery used for:
--		- run tracking (success/failure, timings)
--		- basic pipeline observability
--		- for auditing what ran and when
--		- debug failures		

CREATE SCHEMA IF NOT EXISTS ops;

-- ops.pipeline_runs
-- One row per pipeline/job execution
-- This supports monitoring and reproducibility.
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
	-- Unique identifier for a run
	run_id STRING,

	-- Job name, intend: 'ingest_sst', 'ingest_chl', 'train_iforest', 'score_iforest'
	job_name STRING,

	-- Start/end timestamps for the job run
	start_ts TIMESTAMP,
	end_ts TIMESTAMP,

	-- Status string, intend: 'SUCCESS' or 'FAILED'
	status STRING,

	-- Number of rows written to the destination table (for quality check)
	rows_written INT64,

	-- Intended for free-text notes: error snippets, parameters used
	notes STRING
)
PARTITION BY DATE(start_ts);
