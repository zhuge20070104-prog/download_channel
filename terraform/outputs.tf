# terraform/outputs.tf

# ─── Networking ───

output "vpc_id" {
  value = module.networking.vpc_id
}

output "private_subnet_ids" {
  value = module.networking.private_subnet_ids
}

# ─── Storage ───

output "bronze_bucket_name" {
  value = module.storage.bronze_bucket_name
}

output "silver_bucket_name" {
  value = module.storage.silver_bucket_name
}

output "scripts_bucket_name" {
  value = module.storage.scripts_bucket_name
}

# ─── DynamoDB ───

output "checkpoint_table_name" {
  value = module.dynamodb.checkpoint_table_name
}

# ─── Compute ───

output "glue_workflow_name" {
  value = module.glue_etl.workflow_name
}

output "glue_bronze_job_name" {
  value = module.glue_etl.bronze_job_name
}

output "glue_silver_job_name" {
  value = module.glue_etl.silver_job_name
}

output "dlq_replay_job_name" {
  value = module.glue_dlq_replay.dlq_replay_job_name
}

# ─── Observability ───

output "sns_alert_topic_arn" {
  value = module.observability.sns_alert_topic_arn
}

# ─── Snowflake ───

output "snowflake_storage_integration_iam_user_arn" {
  description = "Snowflake Storage Integration IAM User ARN (needed for Phase 2 trust policy)"
  value       = module.snowflake.storage_aws_iam_user_arn
}

output "snowflake_storage_external_id" {
  description = "Snowflake Storage Integration External ID"
  value       = module.snowflake.storage_aws_external_id
}
