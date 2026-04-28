# modules/glue_dlq_replay/main.tf

resource "aws_glue_job" "dlq_replay" {
  name     = "iodp-dc-dlq-replay-${var.environment}"
  role_arn = var.glue_execution_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/glue/dlq_replay.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-auto-scaling"                = "true"
    "--enable-metrics"                     = "true"
    "--enable-continuous-cloudwatch-log"    = "true"
    "--extra-py-files"                     = "s3://${var.scripts_bucket_name}/glue/lib.zip"
    "--BRONZE_BUCKET"                      = var.bronze_bucket_name
    "--DROPZONE_BUCKET"                    = var.dropzone_bucket_name
    "--ENVIRONMENT"                        = var.environment
    "--FAILED_AT_DATE"                     = "PLACEHOLDER"
  }

  tags = var.tags
}
