# modules/glue_etl/main.tf

# ════════════════════════════════════════════════════════════════
#  IAM Role for Glue Jobs
# ════════════════════════════════════════════════════════════════

resource "aws_iam_role" "glue_dc_execution" {
  name = "iodp-dc-glue-execution-${var.environment}"

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

resource "aws_iam_role_policy" "glue_dc_policy" {
  name = "iodp-dc-glue-policy-${var.environment}"
  role = aws_iam_role.glue_dc_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadWriteBronzeSilverScripts"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.bronze_bucket_arn,
          "${var.bronze_bucket_arn}/*",
          var.silver_bucket_arn,
          "${var.silver_bucket_arn}/*",
          var.scripts_bucket_arn,
          "${var.scripts_bucket_arn}/*",
        ]
      },
      {
        Sid    = "S3ReadOnlyDropzone"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${var.dropzone_bucket_name}",
          "arn:aws:s3:::${var.dropzone_bucket_name}/*",
        ]
      },
      {
        Sid    = "DynamoDBCheckpoint"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:DeleteItem",
        ]
        Resource = [var.checkpoint_table_arn]
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
        ]
        Resource = ["*"]
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
        Sid      = "SNSPublish"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [var.sns_alert_topic_arn]
      },
    ]
  })
}

# ════════════════════════════════════════════════════════════════
#  Bronze Glue Job
# ════════════════════════════════════════════════════════════════

resource "aws_glue_job" "bronze_etl" {
  name     = "iodp-dc-bronze-etl-${var.environment}"
  role_arn = aws_iam_role.glue_dc_execution.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/glue/bronze_etl.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = var.glue_dpu
  worker_type       = "G.1X"
  timeout           = var.glue_timeout_minutes

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-auto-scaling"                = "true"
    "--enable-metrics"                     = "true"
    "--enable-continuous-cloudwatch-log"    = "true"
    "--extra-py-files"                     = "s3://${var.scripts_bucket_name}/glue/lib.zip"
    "--DROPZONE_BUCKET"                    = var.dropzone_bucket_name
    "--BRONZE_BUCKET"                      = var.bronze_bucket_name
    "--CHECKPOINT_TABLE"                   = var.checkpoint_table_name
    "--SNS_TOPIC_ARN"                      = var.sns_alert_topic_arn
    "--ENVIRONMENT"                        = var.environment
  }

  tags = var.tags
}

# ════════════════════════════════════════════════════════════════
#  Silver Glue Job
# ════════════════════════════════════════════════════════════════

resource "aws_glue_job" "silver_etl" {
  name     = "iodp-dc-silver-etl-${var.environment}"
  role_arn = aws_iam_role.glue_dc_execution.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/glue/silver_etl.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = var.glue_dpu
  worker_type       = "G.1X"
  timeout           = var.glue_timeout_minutes

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-auto-scaling"                = "true"
    "--enable-metrics"                     = "true"
    "--enable-continuous-cloudwatch-log"    = "true"
    "--extra-py-files"                     = "s3://${var.scripts_bucket_name}/glue/lib.zip"
    "--BRONZE_BUCKET"                      = var.bronze_bucket_name
    "--SILVER_BUCKET"                      = var.silver_bucket_name
    "--CHECKPOINT_TABLE"                   = var.checkpoint_table_name
    "--SNS_TOPIC_ARN"                      = var.sns_alert_topic_arn
    "--ENVIRONMENT"                        = var.environment
  }

  tags = var.tags
}

# ════════════════════════════════════════════════════════════════
#  Glue Workflow: Bronze → Silver
# ════════════════════════════════════════════════════════════════

resource "aws_glue_workflow" "dc_etl" {
  name = "dc-etl-workflow-${var.environment}"
  tags = var.tags
}

resource "aws_glue_trigger" "bronze_start" {
  name          = "dc-bronze-start-${var.environment}"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.dc_etl.name

  actions {
    job_name = aws_glue_job.bronze_etl.name
  }

  tags = var.tags
}

resource "aws_glue_trigger" "silver_after_bronze" {
  name          = "dc-silver-after-bronze-${var.environment}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.dc_etl.name

  predicate {
    conditions {
      job_name = aws_glue_job.bronze_etl.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.silver_etl.name
  }

  tags = var.tags
}

# ════════════════════════════════════════════════════════════════
#  EventBridge: Daily trigger at UTC 10:00
# ════════════════════════════════════════════════════════════════

resource "aws_iam_role" "eventbridge_glue" {
  name = "iodp-dc-eventbridge-glue-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "eventbridge_glue" {
  name = "iodp-dc-eventbridge-glue-policy-${var.environment}"
  role = aws_iam_role.eventbridge_glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:notifyEvent"]
        Resource = [aws_glue_workflow.dc_etl.arn]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "daily_etl" {
  name                = "iodp-dc-daily-etl-trigger-${var.environment}"
  description         = "Daily trigger for Download Channel ETL Workflow at UTC 10:00"
  schedule_expression = "cron(0 10 * * ? *)"
  state               = var.triggers_enabled ? "ENABLED" : "DISABLED"

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "daily_etl" {
  rule     = aws_cloudwatch_event_rule.daily_etl.name
  arn      = aws_glue_workflow.dc_etl.arn
  role_arn = aws_iam_role.eventbridge_glue.arn
}
