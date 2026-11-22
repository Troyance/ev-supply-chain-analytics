#!/usr/bin/env bash
# lands the latest nhtsa recall snapshot in the bronze volume, stamped with today's date.
# plays the part fivetran or a scheduled fetcher would: databricks serverless here can't reach the internet.
# auto loader (notebooks/02_ingest_nhtsa_recalls.py) picks up anything new in the folder.
set -euo pipefail
PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
STAMP=$(date +%Y%m%d)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

curl -sf -A "Mozilla/5.0" -o "$TMP/rcl.zip" https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_POST_2010.zip
unzip -q -o "$TMP/rcl.zip" -d "$TMP"
databricks fs mkdir "dbfs:/Volumes/workspace/ev_bronze/landing/nhtsa/recalls" -p "$PROFILE"
databricks fs cp "$TMP/FLAT_RCL_POST_2010.txt" \
  "dbfs:/Volumes/workspace/ev_bronze/landing/nhtsa/recalls/FLAT_RCL_POST_2010_${STAMP}.txt" --overwrite -p "$PROFILE"
echo "landed FLAT_RCL_POST_2010_${STAMP}.txt"
