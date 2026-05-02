#!/usr/bin/env bash
# scripts/apply_snowflake_sql.sh
# Renders ${ENV}, ${ENV_LOWER}, ${AWS_ACCOUNT_ID}, ${ALERT_EMAIL} placeholders
# and executes snowflake_sql/01-08 via snowsql.
#
# Two preflight checks run before the deploy loop:
#
#   1. ALERT_EMAIL is bound to a Snowflake user.
#      SYSTEM$SEND_EMAIL only delivers to verified user emails — not to
#      arbitrary addresses. We verify the email is at least *registered*;
#      the verification-link click is still the operator's responsibility.
#
#   2. No stateful objects from a prior deploy exist (PIPE / DYNAMIC TABLE /
#      TASK / NOTIFICATION INTEGRATION / ALERT). Re-creating these has side
#      effects (load-history reset, full-refresh credits, alert downtime).
#      Pass --force / -f to bypass — required when intentionally redeploying.
#
# Usage:
#   bash apply_snowflake_sql.sh [--force] <ENVIRONMENT> <AWS_ACCOUNT_ID> [ALERT_EMAIL]

set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 [--force] <ENVIRONMENT> <AWS_ACCOUNT_ID> [ALERT_EMAIL]

Positional:
  ENVIRONMENT       dev | staging | prod
  AWS_ACCOUNT_ID    e.g. 123456789012
  ALERT_EMAIL       Snowflake-verified email for SYSTEM\$SEND_EMAIL
                    (or set ALERT_EMAIL env var)

Flags:
  --force, -f       Bypass the "stateful object already exists" safety gate.
                    Required after the first successful deploy of an env, or
                    whenever you intentionally want to redeploy and accept
                    the side effects (load-history reset, full refresh, etc).

  --help, -h        Show this help.
EOF
}

# ─── Argument parsing ───────────────────────────────────────────────────
FORCE=0
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force|-f) FORCE=1; shift ;;
        --help|-h)  usage; exit 0 ;;
        --)         shift; POSITIONAL+=("$@"); break ;;
        -*)         echo "ERROR: unknown flag '$1'" >&2; usage >&2; exit 2 ;;
        *)          POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]}"

ENV="${1:-}"
AWS_ACCOUNT_ID="${2:-}"
ALERT_EMAIL="${3:-${ALERT_EMAIL:-}}"

if [[ -z "${ENV}" || -z "${AWS_ACCOUNT_ID}" ]]; then
    usage >&2
    exit 2
fi

if [[ -z "${ALERT_EMAIL}" ]]; then
    cat >&2 <<EOF
ERROR: ALERT_EMAIL not provided.
       Pass it as 3rd arg or via the ALERT_EMAIL env var.
       Example: bash $0 dev 123456789012 ops-oncall@example.com
EOF
    exit 1
fi

ENV_UPPER=$(echo "${ENV}" | tr '[:lower:]' '[:upper:]')
ENV_LOWER=$(echo "${ENV}" | tr '[:upper:]' '[:lower:]')

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SQL_DIR="${SCRIPT_DIR}/snowflake_sql"

# ─── Helpers ────────────────────────────────────────────────────────────
# probe_snowsql: run a query that may legitimately fail (e.g. SHOW in a
# schema that does not yet exist on first deploy). Always returns 0; we
# parse markers from the captured output.
probe_snowsql() {
    snowsql \
        -o exit_on_error=false \
        -o output_format=plain \
        -o header=false \
        -o timing=false \
        -o friendly=false \
        "$@" 2>&1 || true
}

extract_marker() {
    # extract value following ::MARKER::
    local marker="$1"
    awk -F"${marker}" -v m="${marker}" 'index($0, m) {print $2; exit}' \
        | tr -d '[:space:]'
}

# ─── Preflight 1: ALERT_EMAIL is registered to a Snowflake user ─────────
echo "=== Preflight 1/2: verifying ALERT_EMAIL is bound to a Snowflake user ==="

EMAIL_OUTPUT=$(probe_snowsql -q "
SHOW USERS;
SELECT '::EMAIL::' || IFF(COUNT(*) > 0, 'REGISTERED', 'NOT_REGISTERED')
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE \"email\" = '${ALERT_EMAIL}';
")
EMAIL_STATUS=$(echo "${EMAIL_OUTPUT}" | extract_marker '::EMAIL::')

if [[ "${EMAIL_STATUS}" != "REGISTERED" ]]; then
    cat >&2 <<EOF
ERROR: ALERT_EMAIL '${ALERT_EMAIL}' is not bound to any Snowflake user.

Snowflake's SYSTEM\$SEND_EMAIL only delivers to verified emails of existing
Snowflake users — it cannot send to arbitrary addresses or distribution lists
that are not bound to a user.

To fix:
  1. Bind the email to a Snowflake user (run as SECURITYADMIN or higher):

       -- Option A: rebind an existing user
       ALTER USER <username> SET EMAIL='${ALERT_EMAIL}';

       -- Option B: create a dedicated alerts user
       CREATE USER alerts_recipient
         EMAIL='${ALERT_EMAIL}'
         MUST_CHANGE_PASSWORD=FALSE
         DEFAULT_ROLE=PUBLIC;

  2. Snowflake emails a verification link to ${ALERT_EMAIL}.
     Click that link.

  3. Re-run this script.

Note: this preflight only checks *registration*. If the verification link is
not clicked, deployment will succeed but alerts will silently fail at
runtime (SYSTEM\$SEND_EMAIL returns OK, no email arrives).

(snowsql output for diagnosis:)
${EMAIL_OUTPUT}
EOF
    exit 1
fi
echo "  ✓ ALERT_EMAIL is registered (operator must still verify the link)"
echo ""

# ─── Preflight 2: stateful object existence check ───────────────────────
# Each entry: TYPE|SHOW_QUERY|HUMAN_NAME|SQL_FILE
# SHOW_QUERY is fed to RESULT_SCAN; we count rows. If schema/db doesn't
# exist (first deploy), the SHOW errors out, COUNT marker is missing,
# we treat as 0 → safe to proceed.
declare -a STATEFUL_PROBES=(
  "PIPE|SHOW PIPES LIKE 'PIPE_DC_WIDE' IN SCHEMA IODP_DC_${ENV_UPPER}.RAW_STAGE|IODP_DC_${ENV_UPPER}.RAW_STAGE.PIPE_DC_WIDE|04_pipe.sql"
  "DYNAMIC TABLE|SHOW DYNAMIC TABLES IN SCHEMA IODP_DC_${ENV_UPPER}.GOLD|IODP_DC_${ENV_UPPER}.GOLD.DC_DAILY_BY_*, DC_PAID_VS_ORGANIC_TREND|05_gold_dynamic_tables.sql"
  "TASK|SHOW TASKS LIKE 'IODP_DC_DEDUP_${ENV_UPPER}' IN SCHEMA IODP_DC_${ENV_UPPER}.SILVER|IODP_DC_${ENV_UPPER}.SILVER.IODP_DC_DEDUP_${ENV_UPPER}|06_dedup_task.sql"
  "NOTIFICATION INTEGRATION|SHOW INTEGRATIONS LIKE 'IODP_DC_EMAIL_NOTIF_${ENV_UPPER}'|IODP_DC_EMAIL_NOTIF_${ENV_UPPER}|08_freshness_alert.sql"
  "ALERT|SHOW ALERTS IN SCHEMA IODP_DC_${ENV_UPPER}.PUBLIC|IODP_DC_SNOWPIPE_FRESHNESS_${ENV_UPPER}, IODP_DC_DYNAMIC_TABLE_LAG_${ENV_UPPER}|08_freshness_alert.sql"
)

echo "=== Preflight 2/2: checking for existing stateful objects ==="

# Files that recreate stateful objects — skipped on replay unless --force.
STATEFUL_FILES=("04_pipe.sql" "05_gold_dynamic_tables.sql" "06_dedup_task.sql" "08_freshness_alert.sql")
SKIP_FILES=()

EXISTING_OBJECTS=()
for probe in "${STATEFUL_PROBES[@]}"; do
    IFS='|' read -r OBJ_TYPE SHOW_QUERY OBJ_NAME SQL_FILE <<< "${probe}"

    PROBE_OUTPUT=$(probe_snowsql -q "
${SHOW_QUERY};
SELECT '::COUNT::' || COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
")
    COUNT=$(echo "${PROBE_OUTPUT}" | extract_marker '::COUNT::')
    COUNT=${COUNT:-0}

    if [[ "${COUNT}" -gt 0 ]]; then
        EXISTING_OBJECTS+=("${OBJ_TYPE}: ${OBJ_NAME}  (defined in ${SQL_FILE})  [${COUNT} match$( [[ ${COUNT} -gt 1 ]] && echo es)]")
    fi
done

if [[ ${#EXISTING_OBJECTS[@]} -gt 0 ]]; then
    if [[ ${FORCE} -eq 0 ]]; then
        # Replay-safe mode: skip stateful files, run safe ones, exit 0 so that
        # `make init` can proceed past this phase without intervention.
        SKIP_FILES=("${STATEFUL_FILES[@]}")

        cat >&2 <<EOF
WARNING: stateful objects from a prior deploy already exist. Skipping the
files that would recreate them so that re-running \`make init\` is safe:

Existing objects:
$(printf '  - %s\n' "${EXISTING_OBJECTS[@]}")

Will SKIP (need --force / FORCE=1 to redeploy):
$(printf '  - %s\n' "${STATEFUL_FILES[@]}")

Will STILL RUN (idempotent — IF NOT EXISTS or stateless VIEW):
  - 01_database_schemas.sql
  - 03_silver_table.sql
  - 07_bi_view.sql

To force a full redeploy (changed alert email, Pipe definition, schema, etc.):
  bash $0 --force ${ENV} ${AWS_ACCOUNT_ID} ${ALERT_EMAIL}
  # or:
  make apply-snowflake-sql ENV=${ENV_LOWER} FORCE=1

Side effects of --force:
  - PIPE                     → resets COPY load history
  - DYNAMIC TABLE            → triggers a full refresh (extra credits)
  - TASK / ALERT             → ~ms gap during recreate
  - NOTIFICATION INTEGRATION → recreate (ALLOWED_RECIPIENTS picks up
                               new alarm_email if it changed)
EOF
    else
        echo "  ⚠ ${#EXISTING_OBJECTS[@]} stateful object(s) exist; --force passed, will redeploy:"
        printf '    - %s\n' "${EXISTING_OBJECTS[@]}"
    fi
else
    echo "  ✓ no prior stateful objects detected (fresh deploy)"
fi
echo ""

# ─── Main deploy loop ───────────────────────────────────────────────────
echo "=== Applying Snowflake SQL (ENV=${ENV_UPPER}, ACCOUNT=${AWS_ACCOUNT_ID}) ==="

for sql_file in $(ls "${SQL_DIR}"/[0-9]*.sql | sort); do
    filename=$(basename "${sql_file}")

    SHOULD_SKIP=0
    for skip in "${SKIP_FILES[@]:-}"; do
        if [[ "${filename}" == "${skip}" ]]; then
            SHOULD_SKIP=1
            break
        fi
    done
    if [[ ${SHOULD_SKIP} -eq 1 ]]; then
        echo "--- Skipping ${filename} (stateful objects exist; pass FORCE=1 to redeploy) ---"
        continue
    fi

    echo "--- Executing ${filename} ---"

    sed \
        -e "s/\${ENV}/${ENV_UPPER}/g" \
        -e "s/\${ENV_LOWER}/${ENV_LOWER}/g" \
        -e "s/\${AWS_ACCOUNT_ID}/${AWS_ACCOUNT_ID}/g" \
        -e "s/\${ALERT_EMAIL}/${ALERT_EMAIL}/g" \
        "${sql_file}" | \
    snowsql -o exit_on_error=true -o output_format=plain -o header=false -o timing=true

    echo "--- Done: ${filename} ---"
done

echo "=== All Snowflake SQL applied successfully ==="
