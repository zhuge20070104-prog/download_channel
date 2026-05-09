# modules/dropzone_seeder/outputs.tf

output "seeder_job_name" {
  value = aws_glue_job.dropzone_seeder.name
}

output "seeder_role_arn" {
  value = aws_iam_role.dropzone_seeder.arn
}
