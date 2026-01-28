#!/usr/bin/env bash
set -euo pipefail

# provision_bigquery.sh usage:
#	provision BigQuery datasets with defined location and create tables from SQL files.

PROJECT_ID="${1:-}"
LOCATION="asia-southeast2"  # Jakarta

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Usage: $0 <GCP_PROJECT_ID>"
  exit 1
fi

echo "Using project: ${PROJECT_ID}"
echo "Using BigQuery location: ${LOCATION}"

# Create datasets (if exists will return an error, just ignore it)
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" mk -d ops || true
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" mk -d standard || true

# Create tables
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < sql/create_ops_tables.sql
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < sql/create_standard_tables.sql

echo "Done."
