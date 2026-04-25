#!/usr/bin/env bash
# scripts/apply_athena_ddl.sh
# Renders placeholders and runs Athena DDL via AWS CLI

set -euo pipefail

ENVIRONMENT="${1:?Usage: $0 <ENVIRONMENT> <AWS_ACCOUNT_ID> [AWS_REGION]}"
ACCOUNT_ID="${2:?Usage: $0 <ENVIRONMENT> <AWS_ACCOUNT_ID> [AWS_REGION]}"
AWS_REGION="${3:-us-east-1}"

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DDL_DIR="${SCRIPT_DIR}/athena_ddl"
OUTPUT_LOC="s3://iodp-dc-bronze-${ENVIRONMENT}-${ACCOUNT_ID}/athena-results/"

echo "=== Applying Athena DDL (ENV=${ENVIRONMENT}, ACCOUNT=${ACCOUNT_ID}) ==="

for ddl_file in "${DDL_DIR}"/*.sql; do
    filename=$(basename "${ddl_file}")
    echo "--- Executing ${filename} ---"

    rendered=$(sed \
        -e "s/\${ENVIRONMENT}/${ENVIRONMENT}/g" \
        -e "s/\${ACCOUNT_ID}/${ACCOUNT_ID}/g" \
        "${ddl_file}")

    # Split on semicolons and execute each statement
    IFS=';' read -ra STATEMENTS <<< "${rendered}"
    for stmt in "${STATEMENTS[@]}"; do
        trimmed=$(echo "${stmt}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
        # Skip empty or comment-only statements
        if [ -z "${trimmed}" ] || [[ "${trimmed}" == --* ]]; then
            continue
        fi

        QUERY_ID=$(aws athena start-query-execution \
            --query-string "${trimmed}" \
            --result-configuration "OutputLocation=${OUTPUT_LOC}" \
            --region "${AWS_REGION}" \
            --output text --query 'QueryExecutionId')

        echo "  Query started: ${QUERY_ID}"

        # Wait for completion
        while true; do
            STATUS=$(aws athena get-query-execution \
                --query-execution-id "${QUERY_ID}" \
                --region "${AWS_REGION}" \
                --output text --query 'QueryExecution.Status.State')
            case "${STATUS}" in
                SUCCEEDED) echo "  Succeeded"; break ;;
                FAILED|CANCELLED)
                    REASON=$(aws athena get-query-execution \
                        --query-execution-id "${QUERY_ID}" \
                        --region "${AWS_REGION}" \
                        --output text --query 'QueryExecution.Status.StateChangeReason')
                    echo "  FAILED: ${REASON}"
                    exit 1
                    ;;
                *) sleep 2 ;;
            esac
        done
    done

    echo "--- Done: ${filename} ---"
done

echo "=== All Athena DDL applied successfully ==="
