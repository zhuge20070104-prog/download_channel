#!/usr/bin/env bash
# scripts/upload_glue_scripts.sh
# Packages glue/lib/ into lib.zip and uploads all Glue scripts to S3

set -euo pipefail

SCRIPTS_BUCKET="${1:?Usage: $0 <scripts-bucket-name>}"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== Packaging glue/lib/ into lib.zip ==="
cd "${SCRIPT_DIR}/glue"
rm -f lib.zip
zip -r lib.zip lib/ -x "lib/__pycache__/*"
cd "${SCRIPT_DIR}"

echo "=== Uploading Glue scripts to s3://${SCRIPTS_BUCKET}/glue/ ==="

aws s3 cp glue/bronze_etl.py    "s3://${SCRIPTS_BUCKET}/glue/bronze_etl.py"
aws s3 cp glue/silver_etl.py    "s3://${SCRIPTS_BUCKET}/glue/silver_etl.py"
aws s3 cp glue/dlq_replay.py    "s3://${SCRIPTS_BUCKET}/glue/dlq_replay.py"
aws s3 cp glue/seed_dropzone.py "s3://${SCRIPTS_BUCKET}/glue/seed_dropzone.py"
aws s3 cp glue/lib.zip          "s3://${SCRIPTS_BUCKET}/glue/lib.zip"

echo "=== Done. Uploaded scripts to s3://${SCRIPTS_BUCKET}/glue/ ==="
