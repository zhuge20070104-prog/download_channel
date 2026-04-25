# modules/glue_etl/outputs.tf

output "glue_execution_role_arn" {
  value = aws_iam_role.glue_dc_execution.arn
}

output "bronze_job_name" {
  value = aws_glue_job.bronze_etl.name
}

output "silver_job_name" {
  value = aws_glue_job.silver_etl.name
}

output "glue_job_names" {
  value = [
    aws_glue_job.bronze_etl.name,
    aws_glue_job.silver_etl.name,
  ]
}

output "workflow_name" {
  value = aws_glue_workflow.dc_etl.name
}
