# modules/snowflake/outputs.tf

output "database_name" {
  value = snowflake_database.dc.name
}

output "raw_stage_schema" {
  value = snowflake_schema.raw_stage.name
}

output "silver_schema" {
  value = snowflake_schema.silver.name
}

output "gold_schema" {
  value = snowflake_schema.gold.name
}

output "warehouse_name" {
  value = snowflake_warehouse.dc.name
}

output "load_role_name" {
  value = snowflake_account_role.load.name
}

output "transform_role_name" {
  value = snowflake_account_role.transform.name
}

output "reader_role_name" {
  value = snowflake_account_role.reader.name
}

output "storage_integration_name" {
  value = snowflake_storage_integration.s3_int.name
}

output "storage_aws_iam_user_arn" {
  description = "Snowflake IAM user ARN for trust policy"
  value       = snowflake_storage_integration.s3_int.storage_aws_iam_user_arn
}

output "storage_aws_external_id" {
  description = "Snowflake external ID for trust policy"
  value       = snowflake_storage_integration.s3_int.storage_aws_external_id
}
