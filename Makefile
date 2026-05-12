# Download Channel ETL — Makefile
# Usage: make <target> ENV=dev [AWS_ACCOUNT_ID=123456789012] [DT=2026-04-25]

.DEFAULT_GOAL := help
SHELL := /bin/bash

ENV            ?= dev
TF_DIR         := terraform
TF_VARS        := -var-file=environments/$(ENV).tfvars
# AWS_REGION 默认从 tfvars 读出来，避免 us-east-1 / ap-southeast-1 不一致。
# 仍可显式 override: make <target> ENV=dev AWS_REGION=us-west-2
AWS_REGION     ?= $(shell grep -E '^\s*aws_region' $(TF_DIR)/environments/$(ENV).tfvars 2>/dev/null | sed -E 's/.*=\s*"([^"]+)".*/\1/')
AWS_ACCOUNT_ID ?= $(shell aws sts get-caller-identity --query Account --output text 2>/dev/null)

# ════════════════════════════════════════════════════════════════
#  Preflight
# ════════════════════════════════════════════════════════════════

.PHONY: check-tools
check-tools: ## Check required CLI tools
	@echo "Checking required tools..."
	@command -v aws       >/dev/null 2>&1 || { echo "ERROR: aws CLI not found"; exit 1; }
	@command -v terraform >/dev/null 2>&1 || { echo "ERROR: terraform not found"; exit 1; }
	@command -v snowsql   >/dev/null 2>&1 || { echo "ERROR: snowsql not found"; exit 1; }
	@command -v python3   >/dev/null 2>&1 || { echo "ERROR: python3 not found"; exit 1; }
	@command -v zip       >/dev/null 2>&1 || { echo "ERROR: zip not found"; exit 1; }
	@echo "All tools OK."

.PHONY: check-aws
check-aws: ## Validate AWS credentials
	@aws sts get-caller-identity > /dev/null || { echo "ERROR: AWS credentials invalid"; exit 1; }
	@echo "AWS credentials OK: $(shell aws sts get-caller-identity --query Arn --output text)"

.PHONY: check-snowflake
check-snowflake: ## Validate Snowflake credentials
	@test -n "$$SNOWFLAKE_USER"     || { echo "ERROR: SNOWFLAKE_USER not set"; exit 1; }
	@test -n "$$SNOWFLAKE_PASSWORD" || { echo "ERROR: SNOWFLAKE_PASSWORD not set"; exit 1; }
	@test -n "$$SNOWFLAKE_ACCOUNT"  || { echo "ERROR: SNOWFLAKE_ACCOUNT not set"; exit 1; }
	@echo "Snowflake env vars OK."

# ════════════════════════════════════════════════════════════════
#  Bootstrap (one-time, before first `make init`)
# ════════════════════════════════════════════════════════════════

.PHONY: bootstrap-tf-backend
bootstrap-tf-backend: ## One-time: create Terraform state bucket (S3 native locking, no DynamoDB)
	@BUCKET="iodp-terraform-state-$(ENV)"; \
	REGION=$$(awk -F'"' '/^region/{print $$2; exit}' $(TF_DIR)/environments/backend-$(ENV).hcl); \
	if [ -z "$$REGION" ]; then \
		echo "ERROR: could not read region from $(TF_DIR)/environments/backend-$(ENV).hcl"; exit 1; \
	fi; \
	echo "=== Bootstrapping Terraform backend (bucket=$$BUCKET, region=$$REGION) ==="; \
	if aws s3api head-bucket --bucket "$$BUCKET" 2>/dev/null; then \
		echo "  ✓ Bucket $$BUCKET already exists, skipping creation"; \
	else \
		echo "  -> Creating bucket..."; \
		if [ "$$REGION" = "us-east-1" ]; then \
			aws s3api create-bucket --bucket "$$BUCKET" --region "$$REGION" > /dev/null; \
		else \
			aws s3api create-bucket --bucket "$$BUCKET" --region "$$REGION" \
				--create-bucket-configuration LocationConstraint="$$REGION" > /dev/null; \
		fi; \
		echo "  -> Enabling versioning..."; \
		aws s3api put-bucket-versioning --bucket "$$BUCKET" \
			--versioning-configuration Status=Enabled; \
		echo "  -> Enabling AES256 encryption..."; \
		aws s3api put-bucket-encryption --bucket "$$BUCKET" \
			--server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'; \
		echo "  -> Blocking public access..."; \
		aws s3api put-public-access-block --bucket "$$BUCKET" \
			--public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"; \
		echo "  ✓ Bucket created with versioning + AES256 + public access blocked"; \
	fi; \
	echo "  i State locking uses S3 native (use_lockfile=true); no DynamoDB needed"

# ════════════════════════════════════════════════════════════════
#  Glue Scripts
# ════════════════════════════════════════════════════════════════

.PHONY: upload-glue-scripts
upload-glue-scripts: ## Package and upload Glue scripts to S3
	@BUCKET=$$(cd $(TF_DIR) && terraform output -raw scripts_bucket_name 2>/dev/null || echo "iodp-dc-scripts-$(ENV)-$(AWS_ACCOUNT_ID)"); \
	bash scripts/upload_glue_scripts.sh "$$BUCKET"

# ════════════════════════════════════════════════════════════════
#  Full Deploy (6 phases)
# ════════════════════════════════════════════════════════════════

.PHONY: init
init: check-tools check-aws check-snowflake deploy-infra-phase1 upload-glue-scripts apply-snowflake-sql deploy-infra-phase2 apply-athena-ddl ## Full 6-phase deploy
	@echo ""
	@echo "========================================="
	@echo "  Deploy complete! Verify with:"
	@echo "    make run-etl ENV=$(ENV)"
	@echo "========================================="

.PHONY: deploy-infra-phase1
deploy-infra-phase1: ## Phase 1: TF apply (infra + Snowflake + snowpipe AWS role, no Glue triggers)
	# module.snowpipe MUST be in phase 1: it creates the IAM role that
	# Snowflake's CREATE PIPE (in 04_pipe.sql) AssumeRoles. Without it,
	# 04_pipe.sql fails with "User ... is not authorized to perform
	# sts:AssumeRole" because the target role does not yet exist.
	# module.observability is pulled in as a transitive dependency of
	# snowpipe (sns_alert_topic_arn); listed explicitly for clarity.
	cd $(TF_DIR) && terraform init -reconfigure -backend-config=environments/backend-$(ENV).hcl && terraform apply \
		$(TF_VARS) \
		-var='triggers_enabled=false' \
		-target=module.networking \
		-target=module.storage \
		-target=module.dynamodb \
		-target=module.glue_catalog \
		-target=module.snowflake \
		-target=module.observability \
		-target=module.snowpipe \
		-auto-approve

.PHONY: apply-snowflake-sql
apply-snowflake-sql: ## Phase 2: Run Snowflake SQL (01-08). Pass FORCE=1 to bypass the "stateful object already exists" safety gate (required for redeploys).
	@ALERT_EMAIL_VAL=$$(grep -E '^\s*alarm_email\s*=' $(TF_DIR)/environments/$(ENV).tfvars | sed -E 's/.*=\s*"([^"]+)".*/\1/'); \
	if [ -z "$$ALERT_EMAIL_VAL" ]; then \
		echo "ERROR: alarm_email not found in $(TF_DIR)/environments/$(ENV).tfvars"; exit 1; \
	fi; \
	FORCE_FLAG=""; \
	if [ "$(FORCE)" = "1" ]; then FORCE_FLAG="--force"; fi; \
	echo "Using alert email: $$ALERT_EMAIL_VAL"; \
	bash scripts/apply_snowflake_sql.sh $$FORCE_FLAG "$(ENV)" "$(AWS_ACCOUNT_ID)" "$$ALERT_EMAIL_VAL"

.PHONY: deploy-infra-phase2
deploy-infra-phase2: ## Phase 3: Full TF apply (triggers enabled). Auto-extracts Snowflake pipe SQS ARN.
	@PIPE_SQS_ARN=$$(bash scripts/get_pipe_sqs_arn.sh $(ENV) 2>/dev/null); \
	EXTRA_VAR=""; \
	if [ -n "$$PIPE_SQS_ARN" ]; then \
		echo "Snowflake AUTO_INGEST SQS ARN: $$PIPE_SQS_ARN"; \
		EXTRA_VAR="-var=snowflake_pipe_sqs_arn=$$PIPE_SQS_ARN"; \
	else \
		echo "NOTE: Snowflake pipe SQS ARN not available — bucket notification queue block will be skipped this apply (run 04_pipe.sql first, or re-run this target after)."; \
	fi; \
	cd $(TF_DIR) && terraform apply $(TF_VARS) $$EXTRA_VAR -auto-approve

.PHONY: apply-athena-ddl
apply-athena-ddl: ## Register Athena tables
	bash scripts/apply_athena_ddl.sh "$(ENV)" "$(AWS_ACCOUNT_ID)" "$(AWS_REGION)"

.PHONY: deploy
deploy: ## Everyday update: full terraform apply
	cd $(TF_DIR) && terraform init -reconfigure -backend-config=environments/backend-$(ENV).hcl && terraform apply $(TF_VARS)

# ════════════════════════════════════════════════════════════════
#  Manual ETL Triggers
# ════════════════════════════════════════════════════════════════

.PHONY: run-etl
run-etl: ## Trigger full Glue Workflow (Bronze → Silver)
	aws glue start-workflow-run --name "dc-etl-workflow-$(ENV)" --region $(AWS_REGION)
	@echo "Workflow started. Monitor: aws glue get-workflow-run --name dc-etl-workflow-$(ENV) --region $(AWS_REGION)"

.PHONY: demo
demo: ## End-to-end demo: seed dropzone → wait → run Bronze→Silver workflow. DT=YYYY-MM-DD optional (default today UTC).
	@DT="$${DT:-$$(date -u +%Y-%m-%d)}"; \
	echo "=== 1/3 Seeding dropzone for dt=$$DT (1000 groups × 4 channels = 4000 rows) ==="; \
	RUN_ID=$$(aws glue start-job-run \
		--job-name "iodp-dc-dropzone-seeder-$(ENV)" \
		--arguments "{\"--TARGET_DT\":\"$$DT\",\"--TARGET_STORE\":\"ios\",\"--ROW_COUNT\":\"1000\",\"--SCENARIO\":\"clean\"}" \
		--region $(AWS_REGION) --query 'JobRunId' --output text); \
	echo "Seed JobRunId: $$RUN_ID"; \
	echo "=== 2/3 Waiting for seed to finish ==="; \
	while true; do \
		STATE=$$(aws glue get-job-run --job-name "iodp-dc-dropzone-seeder-$(ENV)" --run-id "$$RUN_ID" --region $(AWS_REGION) --query 'JobRun.JobRunState' --output text); \
		echo "  state=$$STATE"; \
		case "$$STATE" in \
			SUCCEEDED) break ;; \
			FAILED|STOPPED|TIMEOUT|ERROR) echo "ERROR: seed ended with state=$$STATE"; exit 1 ;; \
		esac; \
		sleep 5; \
	done; \
	echo "=== 3/3 Triggering Bronze → Silver workflow ==="; \
	aws glue start-workflow-run --name "dc-etl-workflow-$(ENV)" --region $(AWS_REGION); \
	echo ""; \
	echo "Done. Monitor with:"; \
	echo "  make status ENV=$(ENV)"

.PHONY: run-bronze
run-bronze: ## Trigger Bronze ETL only (DT=YYYY-MM-DD optional)
	$(eval ARGS := {})
	$(if $(DT),$(eval ARGS := {"--TARGET_DT":"$(DT)"}))
	aws glue start-job-run \
		--job-name "iodp-dc-bronze-etl-$(ENV)" \
		--arguments '$(ARGS)' \
		--region $(AWS_REGION)

.PHONY: run-silver
run-silver: ## Trigger Silver ETL only (DT=YYYY-MM-DD optional)
	$(eval ARGS := {})
	$(if $(DT),$(eval ARGS := {"--TARGET_DT":"$(DT)"}))
	aws glue start-job-run \
		--job-name "iodp-dc-silver-etl-$(ENV)" \
		--arguments '$(ARGS)' \
		--region $(AWS_REGION)

.PHONY: backfill
backfill: ## Backfill date range: make backfill START=2026-04-01 END=2026-04-25 ENV=dev
	@test -n "$(START)" || { echo "ERROR: START date required"; exit 1; }
	@test -n "$(END)"   || { echo "ERROR: END date required"; exit 1; }
	@echo "Backfilling $(START) → $(END)..."
	@current="$(START)"; \
	while [ "$$current" \!= "$$(date -d '$(END) +1 day' '+%Y-%m-%d')" ]; do \
		echo "--- Backfill $$current ---"; \
		aws glue start-job-run \
			--job-name "iodp-dc-bronze-etl-$(ENV)" \
			--arguments "{\"--TARGET_DT\":\"$$current\",\"--BACKFILL_MODE\":\"true\"}" \
			--region $(AWS_REGION); \
		current=$$(date -d "$$current +1 day" '+%Y-%m-%d'); \
	done
	@echo "Backfill jobs submitted."

# ════════════════════════════════════════════════════════════════
#  DLQ
# ════════════════════════════════════════════════════════════════

.PHONY: dlq-review
dlq-review: ## List DLQ files
	aws s3 ls "s3://iodp-dc-bronze-$(ENV)-$(AWS_ACCOUNT_ID)/dead_letter/" --recursive --human-readable

.PHONY: dlq-replay
dlq-replay: ## Replay all DLQ files from a given failure day: make dlq-replay DATE=2026-04-25
	## DATE = the day the failure happened (UTC), i.e. the failed_at=<DATE>
	## prefix in s3://<bronze>/dead_letter/. NOT the business dt of the data —
	## one failure day usually contains failures spanning multiple business dt's.
	@test -n "$(DATE)" || { echo "ERROR: DATE (failed_at day, YYYY-MM-DD) required"; exit 1; }
	aws glue start-job-run \
		--job-name "iodp-dc-dlq-replay-$(ENV)" \
		--arguments "{\"--FAILED_AT_DATE\":\"$(DATE)\"}" \
		--region $(AWS_REGION)
	@echo "DLQ replay job started for failed_at=$(DATE)"

# ════════════════════════════════════════════════════════════════
#  Snowpipe DLQ (delivery-level failures only — see explanation.md)
# ════════════════════════════════════════════════════════════════
# Note: COPY-level failures (schema/parse errors) are NOT here — they go to
# Snowflake's COPY_HISTORY view. This DLQ catches messages Snowpipe failed
# to ack after maxReceiveCount=5 (Snowpipe slow / stuck / IAM issues).

.PHONY: snowpipe-dlq-status
snowpipe-dlq-status: ## Show Snowpipe DLQ message counts
	@QUEUE_URL=$$(cd $(TF_DIR) && terraform output -raw snowpipe_dlq_url 2>/dev/null) || { echo "ERROR: snowpipe_dlq_url output unavailable — run 'make deploy' first"; exit 1; }; \
	aws sqs get-queue-attributes \
		--queue-url "$$QUEUE_URL" \
		--attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible ApproximateAgeOfOldestMessage \
		--region $(AWS_REGION) \
		--output table

.PHONY: snowpipe-dlq-peek
snowpipe-dlq-peek: ## Peek at one DLQ message without consuming it
	@QUEUE_URL=$$(cd $(TF_DIR) && terraform output -raw snowpipe_dlq_url 2>/dev/null) || { echo "ERROR: snowpipe_dlq_url output unavailable"; exit 1; }; \
	aws sqs receive-message \
		--queue-url "$$QUEUE_URL" \
		--max-number-of-messages 1 \
		--visibility-timeout 1 \
		--region $(AWS_REGION)

.PHONY: snowpipe-dlq-redrive
snowpipe-dlq-redrive: ## Move all DLQ messages back to main Snowpipe queue (AWS-native StartMessageMoveTask)
	@DLQ_ARN=$$(cd $(TF_DIR) && terraform output -raw snowpipe_dlq_arn 2>/dev/null) || { echo "ERROR: snowpipe_dlq_arn output unavailable"; exit 1; }; \
	echo "Starting message move task: $$DLQ_ARN -> main queue (default destination)"; \
	aws sqs start-message-move-task \
		--source-arn "$$DLQ_ARN" \
		--region $(AWS_REGION); \
	echo "Move task started. Check status with: make snowpipe-dlq-redrive-status ENV=$(ENV)"

.PHONY: snowpipe-dlq-redrive-status
snowpipe-dlq-redrive-status: ## Show in-flight / recent redrive tasks for the DLQ
	@DLQ_ARN=$$(cd $(TF_DIR) && terraform output -raw snowpipe_dlq_arn 2>/dev/null) || { echo "ERROR: snowpipe_dlq_arn output unavailable"; exit 1; }; \
	aws sqs list-message-move-tasks \
		--source-arn "$$DLQ_ARN" \
		--region $(AWS_REGION) \
		--output table

.PHONY: snowpipe-dlq-redrive-cancel
snowpipe-dlq-redrive-cancel: ## Cancel an in-flight redrive: pass HANDLE=<task-handle from snowpipe-dlq-redrive-status>
	@test -n "$(HANDLE)" || { echo "ERROR: HANDLE=<task-handle> required (get from snowpipe-dlq-redrive-status)"; exit 1; }
	aws sqs cancel-message-move-task --task-handle "$(HANDLE)" --region $(AWS_REGION)

# ════════════════════════════════════════════════════════════════
#  Status & Info
# ════════════════════════════════════════════════════════════════

.PHONY: status
status: ## Show deployment status
	@echo "=== Terraform Outputs ==="
	@cd $(TF_DIR) && terraform output 2>/dev/null || echo "(terraform not initialized)"
	@echo ""
	@echo "=== Recent Glue Job Runs ==="
	@aws glue get-job-runs --job-name "iodp-dc-bronze-etl-$(ENV)" --max-items 3 --region $(AWS_REGION) \
		--query 'JobRuns[].{Id:Id,State:JobRunState,Start:StartedOn}' --output table 2>/dev/null || true
	@aws glue get-job-runs --job-name "iodp-dc-silver-etl-$(ENV)" --max-items 3 --region $(AWS_REGION) \
		--query 'JobRuns[].{Id:Id,State:JobRunState,Start:StartedOn}' --output table 2>/dev/null || true

# ════════════════════════════════════════════════════════════════
#  Destroy
# ════════════════════════════════════════════════════════════════

.PHONY: destroy
destroy: ## Destroy all infrastructure (with confirmation)
	@echo "WARNING: This will destroy ALL resources in $(ENV)."
	@echo "Snowflake database must be dropped manually after Terraform destroy."
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] || exit 1
	$(MAKE) do-destroy

.PHONY: do-destroy
do-destroy:
	cd $(TF_DIR) && terraform destroy $(TF_VARS)
	@echo ""
	@echo "NOTE: Manually drop Snowflake database:"
	@echo "  DROP DATABASE IODP_DC_$$(echo $(ENV) | tr '[:lower:]' '[:upper:]');"

# ════════════════════════════════════════════════════════════════
#  Misc
# ════════════════════════════════════════════════════════════════

.PHONY: clean
clean: ## Remove local caches
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf $(TF_DIR)/.terraform

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'
