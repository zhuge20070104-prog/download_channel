# Download Channel ETL Pipeline

Data.ai 下载渠道数据 ETL 管线 — 从 S3 Dropzone 到 Snowflake 的 Medallion 架构。

## Architecture

```
  Data.ai
    │  (daily PUT: csv.gz / parquet)
    ▼
┌─────────────────┐
│   S3 Dropzone   │  (external, read-only)
│     narrow/     │
└────────┬────────┘
         │  EventBridge (UTC 10:00 daily)
         ▼
┌─────────────────┐     ┌──────────────┐
│  Glue Bronze    │────▶│  S3 Bronze   │  Parquet, dt/store partitioned
│  (validate +    │     │  narrow/     │
│   dedup + type) │     │  dead_letter/│
└────────┬────────┘     └──────────────┘
         │  Glue Workflow (CONDITIONAL trigger)
         ▼
┌─────────────────┐     ┌──────────────┐
│  Glue Silver    │────▶│  S3 Silver   │  Unified wide Parquet
│  (pivot + DQ)   │     │  dt/store/   │
└─────────────────┘     └──────┬───────┘
                               │  S3 Event → SNS → SQS
                               ▼
                        ┌──────────────┐
                        │  Snowpipe    │  AUTO_INGEST → SILVER.DC_WIDE
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │  Snowflake   │
                        │  Silver Table│  + Daily Dedup Task
                        └──────┬───────┘
                               │  Dynamic Tables (auto-refresh)
                        ┌──────▼───────┐
                        │  Gold DTs    │  DC_DAILY_BY_APP
                        │              │  DC_DAILY_BY_COUNTRY
                        │              │  DC_PAID_VS_ORGANIC_TREND
                        └──────────────┘
```

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| AWS CLI | >= 2.0 | AWS resource management |
| Terraform | >= 1.6 | Infrastructure as Code |
| snowsql | latest | Snowflake SQL execution |
| Python | >= 3.10 | Local development / testing |
| zip | any | Package Glue lib for upload |

## Project Structure

```
download_channel/
├── terraform/                     # Infrastructure as Code
│   ├── backend.tf                 # S3 remote state
│   ├── main.tf                    # Root module (10 sub-modules)
│   ├── variables.tf / outputs.tf
│   ├── locals.tf / versions.tf
│   ├── environments/
│   │   ├── dev.tfvars
│   │   └── prod.tfvars
│   └── modules/
│       ├── networking/            # VPC + subnets + NAT + Glue SG
│       ├── storage/               # 3 S3 buckets + SNS topic
│       ├── dynamodb/              # Checkpoint table
│       ├── glue_catalog/          # Athena Glue databases
│       ├── glue_etl/              # Bronze + Silver jobs + Workflow
│       ├── glue_dlq_replay/       # DLQ replay job
│       ├── snowflake/             # DB + schemas + roles + integration
│       ├── snowpipe/              # IAM trust + SQS + SNS subscription
│       ├── gold_dynamic_tables/   # Grants for Dynamic Tables
│       └── observability/         # Alarms + SNS + Lambda + Dashboard
├── glue/                          # Glue PySpark ETL code
│   ├── bronze_etl.py
│   ├── silver_etl.py
│   ├── dlq_replay.py
│   └── lib/                       # Shared libraries
├── lambda/
│   ├── dlq_weekly_report/         # Weekly DLQ summary
│   ├── stale_lock_check/          # DynamoDB stale lock detector (every 30m)
│   └── dropzone_freshness_check/  # Daily upstream-data-missing detector
├── snowflake_sql/                 # Snowflake DDL (01-08)
├── athena_ddl/                    # Athena external table DDL
├── scripts/                       # Deploy helper scripts
├── Makefile                       # All operations
├── PLAN.md                        # Architecture design doc
└── README.md                      # This file
```

## Quick Start

```bash
# 1. Set environment variables
export AWS_PROFILE=your-profile
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1

# 2. Edit terraform/environments/dev.tfvars with your account ID

# 3. Full deploy (6 phases)
make init ENV=dev

# 4. Verify — manual trigger
make run-etl ENV=dev
```

## One-time Setup: Snowflake User Email for Alerts

`08_freshness_alert.sql` creates an `EMAIL` notification integration that calls `SYSTEM$SEND_EMAIL`. Snowflake will only deliver to addresses that are:

1. **Bound to a Snowflake user** in this account (the `EMAIL` property of some `USER`)
2. **Verified** by clicking the link Snowflake mails to that address

Neither step is automatable from Terraform / SQL — they are operator actions you must do once per environment, before Phase 4. **Without this, deployment succeeds but alerts silently fail at runtime.**

```sql
-- as SECURITYADMIN (or any role with MANAGE GRANTS)
-- Option A: rebind an existing user
ALTER USER <username> SET EMAIL='ops-oncall@example.com';

-- Option B: dedicated alerts user
CREATE USER alerts_recipient
  EMAIL='ops-oncall@example.com'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_ROLE=PUBLIC;
```

Then check the inbox of the address and click the verification link Snowflake sends.

`apply_snowflake_sql.sh`'s **Preflight 1** detects whether the address is at least registered — but it cannot detect whether you have clicked the verification link, so the click is on you. The `alarm_email` value in `terraform/environments/<env>.tfvars` must match the email bound above.

## Deployment Phases

The deploy is split into 6 phases because of a **bidirectional IAM trust** between AWS and Snowflake:

| Phase | Command | What happens |
|-------|---------|-------------|
| 1/6 | `make check-tools check-aws check-snowflake` | Validate prerequisites |
| 2/6 | `make upload-glue-scripts` | Package `glue/lib/` → `lib.zip`, upload to S3 |
| 3/6 | `make deploy-infra-phase1` | Terraform apply (storage, DynamoDB, Snowflake DB/Integration) — gets `STORAGE_AWS_IAM_USER_ARN` |
| 4/6 | `make apply-snowflake-sql` | snowsql runs 01-08: tables, pipe, dynamic tables, dedup task, freshness alerts. Two preflights run first: (1) ALERT_EMAIL is bound to a Snowflake user (hard-fails if not); (2) checks whether stateful objects (PIPE / DYNAMIC TABLE / TASK / NOTIFICATION INTEGRATION / ALERT) from a prior deploy exist. If they do, the four files that recreate them are **skipped** with a warning so that `make init` can replay safely; only the idempotent files (01, 03, 07) re-run. Pass `FORCE=1` to redeploy stateful objects too — see "Re-running phase 4" below. |
| 5/6 | `make deploy-infra-phase2` | Full Terraform apply — creates IAM trust with Snowflake ARN, enables triggers |
| 6/6 | `make run-etl ENV=dev` | Verify end-to-end |

**Why two Terraform phases?** Snowflake's Storage Integration creates an IAM user ARN that must be trusted by the AWS IAM role. Phase 1 creates the integration to get the ARN; Phase 2 uses it in the trust policy.

### Re-running phase 4

The first run of `make apply-snowflake-sql ENV=<env>` works on a clean account and runs all 8 SQL files. On every subsequent run the script detects existing stateful objects from the prior deploy and **skips the four files that would recreate them** (`04_pipe.sql`, `05_gold_dynamic_tables.sql`, `06_dedup_task.sql`, `08_freshness_alert.sql`). The three idempotent files (`01_database_schemas.sql`, `03_silver_table.sql`, `07_bi_view.sql`) still run — they are safe replays (`IF NOT EXISTS` / stateless `VIEW`).

This default is what makes `make init` replay-safe: re-running it will not blow away your Pipe load history, retrigger Dynamic Table full refreshes, or briefly silence alerts.

If you actually changed Pipe / Dynamic Table / Task / Alert / Notification Integration definitions (or the `alarm_email` in tfvars), redeploy with `FORCE=1`:

```bash
make apply-snowflake-sql ENV=dev FORCE=1
```

Side effects when forcing:
- **PIPE**: load history is reset. Files still in the stage may be reprocessed (the dedup task in `06_dedup_task.sql` cleans up duplicates by `_loaded_at`).
- **DYNAMIC TABLE**: triggers a full refresh on next cycle. Burns warehouse credits proportional to upstream `DC_WIDE` size.
- **TASK / ALERT**: brief suspend → recreate → resume gap (~ms). No alerts fire during the gap.
- **NOTIFICATION INTEGRATION**: recreated; `ALLOWED_RECIPIENTS` picks up the current `alarm_email`.

Edge case: if `03_silver_table.sql` already exists and you added/removed columns, `CREATE TABLE IF NOT EXISTS` will not migrate the schema — neither default nor `FORCE=1` mode does this. Run an explicit `ALTER TABLE` migration manually.

## Terraform Modules

| Module | Resources | Purpose |
|--------|-----------|---------|
| `networking` | VPC, subnets, NAT, Glue SG | Network isolation for Glue |
| `storage` | 3 S3 buckets, SNS topic | Bronze + Silver + Scripts, S3 event notification |
| `dynamodb` | DynamoDB table | Checkpoint + distributed locking |
| `glue_catalog` | 2 Glue databases | Athena ad-hoc queries |
| `glue_etl` | IAM role, 2 Glue jobs, Workflow, EventBridge | Core ETL pipeline |
| `glue_dlq_replay` | 1 Glue job | Manual DLQ replay |
| `snowflake` | DB, schemas, warehouse, roles, integration | Snowflake foundation |
| `snowpipe` | IAM role, SQS queue, SNS subscription | S3 → Snowflake bridge |
| `gold_dynamic_tables` | Grants | Permissions for Dynamic Tables |
| `observability` | SNS, CW Alarms, 3 Lambdas, Dashboard | Glue failure alarms + DLQ report + stale-lock + dropzone freshness |

## S3 Layout

```
s3://iodp-dc-bronze-<env>-<acct>/
    download_channel/narrow/dt=.../store=.../    # Narrow Parquet (raw schema preserved)
    dead_letter/YYYY-MM-DD/                      # Failed files + .error.json

s3://iodp-dc-silver-<env>-<acct>/
    download_channel/dt=.../store=.../       # Unified wide Parquet (Snowpipe reads here)

s3://iodp-dc-scripts-<env>-<acct>/
    glue/bronze_etl.py, silver_etl.py, dlq_replay.py, lib.zip
```

**Lifecycle policies:** Bronze/Silver: 30d → Standard-IA → 90d → Glacier IR → 365d delete. Dead letter: 30d delete.

## Snowflake Objects

```
IODP_DC_<ENV>
├── RAW_STAGE
│   ├── STAGE: SILVER_S3_STAGE
│   ├── FILE FORMAT: PARQUET_FF
│   └── PIPE: PIPE_DC_WIDE (AUTO_INGEST)
├── SILVER
│   └── TABLE: DC_WIDE (PK: dt, product_id, app_store, country, device)
└── GOLD
    ├── DYNAMIC TABLE: DC_DAILY_BY_APP (15min lag)
    ├── DYNAMIC TABLE: DC_DAILY_BY_COUNTRY (15min lag)
    └── DYNAMIC TABLE: DC_PAID_VS_ORGANIC_TREND (1hr lag)

Roles: IODP_DC_LOAD_<ENV> (Snowpipe), IODP_DC_TRANSFORM_<ENV> (DT refresh), IODP_DC_READER_<ENV> (BI)
Task: IODP_DC_DEDUP_<ENV> (daily 06:00 UTC — dedup restate window)
```

## Data Quality Checks

Silver ETL runs 5 DQ checks before writing:

| Check | Threshold | Level |
|-------|-----------|-------|
| Row count variance vs Bronze | > 1% | BLOCKING |
| Null rate on critical columns | > 0.1% | BLOCKING |
| Date range (±7 days) | > 0 rows outside | BLOCKING |
| Negative download values | any | WARN |
| Equation: total = featured + organic | > 0.1% | WARN |

Blocking failures route data to DLQ + send SNS alert. Warnings proceed with SNS alert.

## Monitoring & Alerts

All alerts publish to a single SNS topic (`iodp-dc-alerts-<env>`) subscribed to `alarm_email` from tfvars. Snowflake-side alerts use a separate Email Notification Integration.

| # | Alert | Trigger | Implementation |
|---|-------|---------|---------------|
| 1 | Glue Job failure | `numFailedTasks ≥ 1` | CloudWatch Alarm per Glue job → SNS |
| 2 | DLQ new files | `dlq_count > 0` after a Glue run | Inline `send_alert()` in `bronze_etl.py` / `silver_etl.py` |
| 3 | DLQ weekly report | Mondays 09:00 UTC | `lambda/dlq_weekly_report/` via EventBridge cron |
| 4 | Snowpipe missed today | No `COPY_HISTORY` row for `PIPE_DC_WIDE` with `LAST_LOAD_TIME` in today (UTC) | Snowflake `ALERT IODP_DC_SNOWPIPE_FRESHNESS_<ENV>` (daily UTC 13:00) |
| 5 | Stale DynamoDB lock | `status=running AND lock_expires_at < now` | `lambda/stale_lock_check/` every 30 min |
| 6 | Upstream data missing | No files under expected `dt=today` partitions | `lambda/dropzone_freshness_check/` daily 11:00 UTC (1h after ETL) |
| 7 | DQ check failure | Any blocking DQ check fails | Inline `send_alert()` in `silver_etl.py` |

Bonus: `IODP_DC_DYNAMIC_TABLE_LAG_<ENV>` Snowflake Alert flags failed Gold Dynamic Table refreshes.

**⚠ Snowflake email setup** — `SYSTEM$SEND_EMAIL` only delivers to addresses bound to a Snowflake user **and verified** in this account. See [One-time Setup: Snowflake User Email for Alerts](#one-time-setup-snowflake-user-email-for-alerts) above. `apply_snowflake_sql.sh` runs a registration preflight that catches the missing-binding case; the verification-link click is the operator's responsibility. If skipped, alerts #4 (and the Dynamic-Table bonus) silently fail at runtime even though deployment succeeds.

**Tunable schedules** (in `terraform/modules/observability/variables.tf`):
- `stale_lock_check_schedule` (default `rate(30 minutes)`)
- `dropzone_freshness_schedule` (default `cron(0 11 * * ? *)`)
- `expected_dropzone_stores` (default `["ios", "google-play"]`)

## DLQ & Replay

1. **Auto-capture**: Schema mismatches and DQ failures are written to `dead_letter/`
2. **Weekly report**: Lambda scans DLQ every Monday, sends SNS summary
3. **Manual replay**: After fixing root cause:
   ```bash
   make dlq-review                    # List DLQ files
   make dlq-replay DATE=2026-04-25    # Replay specific date
   ```

## Makefile Targets

```bash
make help                 # Show all targets
make init ENV=dev         # Full 6-phase deploy
make deploy ENV=dev       # Everyday terraform apply
make run-etl ENV=dev      # Trigger full Workflow
make run-bronze ENV=dev DT=2026-04-25   # Bronze only
make run-silver ENV=dev DT=2026-04-25   # Silver only
make backfill START=2026-04-01 END=2026-04-25 ENV=dev
make dlq-review           # List DLQ files
make dlq-replay DATE=...  # Replay DLQ
make status               # Show deployment status
make destroy ENV=dev      # Tear down (with confirmation)
```

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `AWS_PROFILE` or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Yes | AWS authentication |
| `SNOWFLAKE_USER` | Yes | Snowflake user for snowsql |
| `SNOWFLAKE_PASSWORD` | Yes | Snowflake password |
| `SNOWFLAKE_ACCOUNT` | Yes | Snowflake account (e.g. `xy12345.us-east-1`) |
| `TF_VAR_snowflake_password` | Yes | Terraform Snowflake provider auth |

## Destroy

```bash
make destroy ENV=dev
```

After Terraform destroy, manually drop the Snowflake database:
```sql
DROP DATABASE IODP_DC_DEV;
```

## Troubleshooting

**Snowpipe not ingesting:**
- Check `SELECT SYSTEM$PIPE_STATUS('PIPE_DC_WIDE');` in Snowflake
- Verify SQS queue receives messages: `aws sqs get-queue-attributes --queue-url <url> --attribute-names ApproximateNumberOfMessagesVisible`
- Verify S3 event notification is configured: `aws s3api get-bucket-notification-configuration --bucket <silver-bucket>`

**Checkpoint lock stuck:**
- Query DynamoDB: `aws dynamodb get-item --table-name iodp-dc-checkpoint-<env> --key '{"partition_key":{"S":"bronze#2026-04-25#ios"}}'`
- If `lock_expires_at` is in the past, the lock will auto-release on next run
- The `stale_lock_check` Lambda (every 30 min) emits an SNS alert listing every partition in this state — check inbox before manually clearing

**Snowflake alert never fires:**
- `SYSTEM$SEND_EMAIL` requires the recipient address to be verified on a Snowflake user — see Monitoring & Alerts section
- Verify alert is enabled: `SHOW ALERTS LIKE 'IODP_DC_%';` (column `state` should be `started`)
- Inspect history: `SELECT * FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 10;`
- Note: `ACCOUNT_USAGE.COPY_HISTORY` has ~45 min latency, so alert window is set to 2h to avoid false positives

**Dropzone freshness Lambda false-positives:**
- If Data.ai routinely uploads after 11:00 UTC, push the schedule later via `dropzone_freshness_schedule` tfvar
- Or set `CHECK_DATE_OFFSET_DAYS=-1` env var on the Lambda to verify yesterday's data instead of today's

**Glue job timeout:**
- Increase `glue_timeout_minutes` in tfvars
- Check CloudWatch logs: `/aws-glue/jobs/output/iodp-dc-bronze-etl-<env>`
