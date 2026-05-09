# Download Channel ETL — Makefile
# Usage: make <target> ENV=dev [AWS_ACCOUNT_ID=123456789012] [DT=2026-04-25]

.DEFAULT_GOAL := help
SHELL := /bin/bash

ENV            ?= dev
AWS_REGION     ?= us-east-1
AWS_ACCOUNT_ID ?= $(shell aws sts get-caller-identity --query Account --output text 2>/dev/null)
TF_DIR         := terraform
TF_VARS        := -var-file=environments/$(ENV).tfvars

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
init: check-tools check-aws check-snowflake upload-glue-scripts deploy-infra-phase1 apply-snowflake-sql deploy-infra-phase2 apply-athena-ddl ## Full 6-phase deploy
	@echo ""
	@echo "========================================="
	@echo "  Deploy complete! Verify with:"
	@echo "    make run-etl ENV=$(ENV)"
	@echo "========================================="

.PHONY: deploy-infra-phase1
deploy-infra-phase1: ## Phase 1: TF apply (infra + Snowflake, no triggers)
	cd $(TF_DIR) && terraform init -reconfigure -backend-config=environments/backend-$(ENV).hcl && terraform apply \
		$(TF_VARS) \
		-var='triggers_enabled=false' \
		-target=module.networking \
		-target=module.storage \
		-target=module.dynamodb \
		-target=module.glue_catalog \
		-target=module.snowflake

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
deploy-infra-phase2: ## Phase 3: Full TF apply (triggers enabled)
	cd $(TF_DIR) && terraform apply $(TF_VARS)

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
