# Download Channel ETL Pipeline

Data.ai 下载渠道数据 ETL 管线 — 从 S3 Dropzone 到 Snowflake 的 Medallion 架构。

## Architecture

```
  Data.ai
    │  (daily PUT: csv.gz / parquet)
    ▼
┌─────────────────┐
│   S3 Dropzone   │  (external, read-only)
│  narrow/ wide/  │
└────────┬────────┘
         │  EventBridge (UTC 10:00 daily)
         ▼
┌─────────────────┐     ┌──────────────┐
│  Glue Bronze    │────▶│  S3 Bronze   │  Parquet, dt/store partitioned
│  (validate +    │     │  v1/ + v2/   │
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
│   └── dlq_weekly_report/         # Weekly DLQ summary Lambda
├── snowflake_sql/                 # Snowflake DDL (01-06)
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

## Deployment Phases

The deploy is split into 6 phases because of a **bidirectional IAM trust** between AWS and Snowflake:

| Phase | Command | What happens |
|-------|---------|-------------|
| 1/6 | `make check-tools check-aws check-snowflake` | Validate prerequisites |
| 2/6 | `make upload-glue-scripts` | Package `glue/lib/` → `lib.zip`, upload to S3 |
| 3/6 | `make deploy-infra-phase1` | Terraform apply (storage, DynamoDB, Snowflake DB/Integration) — gets `STORAGE_AWS_IAM_USER_ARN` |
| 4/6 | `make apply-snowflake-sql` | snowsql runs 01-06: tables, pipe, dynamic tables, dedup task |
| 5/6 | `make deploy-infra-phase2` | Full Terraform apply — creates IAM trust with Snowflake ARN, enables triggers |
| 6/6 | `make run-etl ENV=dev` | Verify end-to-end |

**Why two Terraform phases?** Snowflake's Storage Integration creates an IAM user ARN that must be trusted by the AWS IAM role. Phase 1 creates the integration to get the ARN; Phase 2 uses it in the trust policy.

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
| `observability` | Alarms, SNS, Lambda, Dashboard | Monitoring + DLQ weekly report |

## S3 Layout

```
s3://iodp-dc-bronze-<env>-<acct>/
    download_channel/v1/dt=.../store=.../    # Narrow Parquet
    download_channel/v2/dt=.../store=.../    # Wide Parquet
    dead_letter/YYYY-MM-DD/                  # Failed files + .error.json

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

**Glue job timeout:**
- Increase `glue_timeout_minutes` in tfvars
- Check CloudWatch logs: `/aws-glue/jobs/output/iodp-dc-bronze-etl-<env>`
