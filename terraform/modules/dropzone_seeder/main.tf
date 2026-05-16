# modules/dropzone_seeder/main.tf
#
# Synthetic data generator for the dropzone bucket. For demo / DLQ-path testing.
#
# Why a separate module + role: production semantics treat dropzone as a
# producer-only bucket and the Glue ETL role is read-only on it. The seeder
# uses its own IAM role with PutObject scoped to the narrow data prefix only,
# keeping producer/consumer concerns isolated even in dev.

resource "aws_iam_role" "dropzone_seeder" {
  name = "iodp-dc-dropzone-seeder-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "dropzone_seeder" {
  name = "iodp-dc-dropzone-seeder-policy-${var.environment}"
  role = aws_iam_role.dropzone_seeder.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3WriteDropzoneNarrowOnly"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = [
          "arn:aws:s3:::${var.dropzone_bucket_name}/download_channel/narrow/*",
        ]
      },
      {
        Sid      = "S3ListDropzoneNarrowOnly"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.dropzone_bucket_name}"]
        Condition = {
          StringLike = {
            "s3:prefix" = ["download_channel/narrow/*"]
          }
        }
      },
      {
        Sid    = "S3ReadScripts"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.scripts_bucket_arn,
          "${var.scripts_bucket_arn}/*",
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = ["arn:aws:logs:*:*:*"]
      },
      {
        Sid    = "CloudWatchMetrics"
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
        ]
        Resource = ["*"]
      },
    ]
  })
}

resource "aws_glue_job" "dropzone_seeder" {
  name     = "iodp-dc-dropzone-seeder-${var.environment}"
  role_arn = aws_iam_role.dropzone_seeder.arn

  # Python Shell, not Spark: we only generate <100k rows of synthetic data.
  # 1 DPU pythonshell finishes in seconds and costs ~$0.01 per run.
  command {
    name            = "pythonshell"
    script_location = "s3://${var.scripts_bucket_name}/glue/seed_dropzone.py"
    python_version  = "3.9"
  }

  glue_version = "3.0"
  max_capacity = 1.0
  timeout      = 30

  execution_property {
    max_concurrent_runs = 3
  }

  default_arguments = {
    "--enable-metrics"  = "true"
    "--DROPZONE_BUCKET" = var.dropzone_bucket_name
    "--TARGET_DT"       = "2026-01-01"
    "--TARGET_STORE"    = "ios"
    "--ROW_COUNT"       = "1000"
    "--SCENARIO"        = "clean"
  }

  tags = var.tags
}
