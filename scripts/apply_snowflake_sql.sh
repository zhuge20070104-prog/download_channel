#!/usr/bin/env bash
# scripts/apply_snowflake_sql.sh
# Renders ${ENV}, ${ENV_LOWER}, ${AWS_ACCOUNT_ID} placeholders and executes via snowsql

set -euo pipefail

ENV="${1:?Usage: $0 <ENVIRONMENT> <AWS_ACCOUNT_ID>}"
AWS_ACCOUNT_ID="${2:?Usage: $0 <ENVIRONMENT> <AWS_ACCOUNT_ID>}"

ENV_UPPER=$(echo "${ENV}" | tr '[:lower:]' '[:upper:]')
ENV_LOWER=$(echo "${ENV}" | tr '[:upper:]' '[:lower:]')

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SQL_DIR="${SCRIPT_DIR}/snowflake_sql"

echo "=== Applying Snowflake SQL (ENV=${ENV_UPPER}, ACCOUNT=${AWS_ACCOUNT_ID}) ==="

for sql_file in $(ls "${SQL_DIR}"/[0-9]*.sql | sort); do
    filename=$(basename "${sql_file}")
    echo "--- Executing ${filename} ---"

    sed \
        -e "s/\${ENV}/${ENV_UPPER}/g" \
        -e "s/\${ENV_LOWER}/${ENV_LOWER}/g" \
        -e "s/\${AWS_ACCOUNT_ID}/${AWS_ACCOUNT_ID}/g" \
        "${sql_file}" | \
    snowsql -o exit_on_error=true -o output_format=plain -o header=false -o timing=true

    echo "--- Done: ${filename} ---"
done

echo "=== All Snowflake SQL applied successfully ==="
