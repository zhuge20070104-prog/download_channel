#!/usr/bin/env bash
# scripts/get_pipe_sqs_arn.sh
#
# Print the Snowflake AUTO_INGEST PIPE_DC_WIDE's SQS ARN to stdout.
# Snowflake auto-allocates this SQS in their own AWS account on PIPE creation;
# the ARN is dynamic per pipe and only knowable after the pipe exists.
#
# Used by Makefile's deploy-infra-phase2 target to feed terraform via
# `-var=snowflake_pipe_sqs_arn=<arn>`. If the pipe doesn't exist yet (first
# deploy, before 04_pipe.sql has run), this prints empty and exits 0 — Make
# then skips the -var override, terraform uses the default empty (queue block
# skipped), and the subscription is added on the next deploy.
#
# Usage:
#   bash scripts/get_pipe_sqs_arn.sh <ENV>
#   ARN=$(bash scripts/get_pipe_sqs_arn.sh dev)

set -uo pipefail

ENV="${1:?Usage: $0 <env>}"
ENV_UPPER=$(echo "$ENV" | tr '[:lower:]' '[:upper:]')

# Map SNOWFLAKE_* → SNOWSQL_* (apply_snowflake_sql.sh does the same)
: "${SNOWSQL_ACCOUNT:=${SNOWFLAKE_ACCOUNT:-}}"
: "${SNOWSQL_USER:=${SNOWFLAKE_USER:-}}"
: "${SNOWSQL_PWD:=${SNOWFLAKE_PASSWORD:-}}"
export SNOWSQL_ACCOUNT SNOWSQL_USER SNOWSQL_PWD

if [[ -z "${SNOWSQL_ACCOUNT}" || -z "${SNOWSQL_USER}" || -z "${SNOWSQL_PWD}" ]]; then
    # Quiet exit — Makefile treats empty as "pipe not ready yet, skip wiring".
    exit 0
fi

# Query the pipe status. exit_on_error=false so a missing pipe (first deploy)
# yields no output rather than killing the script.
snowsql \
  -o output_format=plain \
  -o header=false \
  -o timing=false \
  -o friendly=false \
  -o exit_on_error=false \
  -q "USE DATABASE IODP_DC_${ENV_UPPER}; SELECT PARSE_JSON(SYSTEM\$PIPE_STATUS('IODP_DC_${ENV_UPPER}.RAW_STAGE.PIPE_DC_WIDE')):notificationChannelName::STRING" \
  2>/dev/null \
  | grep -Eo 'arn:aws:sqs:[a-z0-9-]+:[0-9]+:sf-snowpipe-[A-Za-z0-9_+-]+' \
  | head -1
