# modules/observability/main.tf

# ════════════════════════════════════════════════════════════════
#  SNS Alert Topic
# ════════════════════════════════════════════════════════════════

resource "aws_sns_topic" "alerts" {
  name = "iodp-dc-alerts-${var.environment}"
  tags = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# ════════════════════════════════════════════════════════════════
#  Glue Job Failure Alerts (EventBridge — Job State Change)
#  监听 job 终态事件 (FAILED/TIMEOUT/ERROR)，而非 task 级 numFailedTasks。
#  原因：task retry 成功 (常见于瞬时 OOM / 节点抖动) 会污染 numFailedTasks，
#  即使 Job 整体 SUCCEEDED 也会触发 task 级告警，产生误报。
# ════════════════════════════════════════════════════════════════

resource "aws_cloudwatch_event_rule" "glue_job_failed" {
  name        = "iodp-dc-glue-job-failed-${var.environment}"
  description = "Catch Glue job terminal failures (FAILED/TIMEOUT/ERROR)"

  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = var.glue_job_names
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })

  state = var.triggers_enabled ? "ENABLED" : "DISABLED"
  tags  = var.tags
}

resource "aws_cloudwatch_event_target" "glue_job_failed_sns" {
  rule = aws_cloudwatch_event_rule.glue_job_failed.name
  arn  = aws_sns_topic.alerts.arn

  input_transformer {
    input_paths = {
      job   = "$.detail.jobName"
      state = "$.detail.state"
      runId = "$.detail.jobRunId"
      msg   = "$.detail.message"
    }
    input_template = "\"Glue job <job> ended in state <state>. RunId=<runId>. Message=<msg>\""
  }
}

resource "aws_sns_topic_policy" "alerts_allow_events" {
  arn = aws_sns_topic.alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowEventBridgePublish"
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.alerts.arn
      },
      {
        Sid       = "AllowCloudWatchAlarmsPublish"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.alerts.arn
      },
    ]
  })
}

# ════════════════════════════════════════════════════════════════
#  DLQ Weekly Report Lambda
# ════════════════════════════════════════════════════════════════

resource "aws_iam_role" "dlq_report_lambda" {
  name = "iodp-dc-dlq-report-lambda-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "dlq_report_lambda" {
  name = "iodp-dc-dlq-report-lambda-policy-${var.environment}"
  role = aws_iam_role.dlq_report_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.bronze_bucket_arn,
          "${var.bronze_bucket_arn}/dead_letter/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.alerts.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = ["arn:aws:logs:*:*:*"]
      },
    ]
  })
}

data "archive_file" "dlq_report" {
  type        = "zip"
  source_dir  = "${path.module}/../../../lambda/dlq_weekly_report"
  output_path = "${path.module}/lambda_dlq_report.zip"
}

resource "aws_lambda_function" "dlq_weekly_report" {
  function_name    = "iodp-dc-dlq-weekly-report-${var.environment}"
  filename         = data.archive_file.dlq_report.output_path
  source_code_hash = data.archive_file.dlq_report.output_base64sha256
  handler          = "handler.handler"
  runtime          = "python3.12"
  timeout          = 120
  memory_size      = 256
  role             = aws_iam_role.dlq_report_lambda.arn

  reserved_concurrent_executions = 1

  environment {
    variables = {
      BRONZE_BUCKET = var.bronze_bucket_name
      SNS_TOPIC_ARN = aws_sns_topic.alerts.arn
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "dlq_report" {
  name              = "/aws/lambda/iodp-dc-dlq-weekly-report-${var.environment}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_cloudwatch_event_rule" "dlq_weekly" {
  name                = "iodp-dc-dlq-weekly-${var.environment}"
  description         = "Weekly DLQ report every Monday UTC 09:00"
  schedule_expression = "cron(0 9 ? * MON *)"
  state               = var.triggers_enabled ? "ENABLED" : "DISABLED"

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "dlq_weekly" {
  rule = aws_cloudwatch_event_rule.dlq_weekly.name
  arn  = aws_lambda_function.dlq_weekly_report.arn
}

resource "aws_lambda_permission" "eventbridge_dlq" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dlq_weekly_report.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.dlq_weekly.arn
}

# ════════════════════════════════════════════════════════════════
#  Stale Lock Check Lambda  (PLAN.md §14 alert #5)
#  Scans DynamoDB checkpoint for status=running with expired lock.
# ════════════════════════════════════════════════════════════════

resource "aws_iam_role" "stale_lock_lambda" {
  name = "iodp-dc-stale-lock-lambda-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "stale_lock_lambda" {
  name = "iodp-dc-stale-lock-lambda-policy-${var.environment}"
  role = aws_iam_role.stale_lock_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Lambda 走 sparse GSI Query，无需 Scan 主表。Resource 只授权 GSI ARN。
        Effect   = "Allow"
        Action   = ["dynamodb:Query"]
        Resource = [var.checkpoint_status_index_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.alerts.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:*:*:*"]
      },
    ]
  })
}

data "archive_file" "stale_lock" {
  type        = "zip"
  source_dir  = "${path.module}/../../../lambda/stale_lock_check"
  output_path = "${path.module}/lambda_stale_lock.zip"
}

resource "aws_lambda_function" "stale_lock_check" {
  function_name    = "iodp-dc-stale-lock-check-${var.environment}"
  filename         = data.archive_file.stale_lock.output_path
  source_code_hash = data.archive_file.stale_lock.output_base64sha256
  handler          = "handler.handler"
  runtime          = "python3.12"
  timeout          = 60
  memory_size      = 256
  role             = aws_iam_role.stale_lock_lambda.arn

  reserved_concurrent_executions = 1

  environment {
    variables = {
      CHECKPOINT_TABLE = var.checkpoint_table_name
      SNS_TOPIC_ARN    = aws_sns_topic.alerts.arn
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "stale_lock" {
  name              = "/aws/lambda/iodp-dc-stale-lock-check-${var.environment}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_cloudwatch_event_rule" "stale_lock" {
  name                = "iodp-dc-stale-lock-${var.environment}"
  description         = "Periodically scan DynamoDB for stale Glue ETL locks"
  schedule_expression = var.stale_lock_check_schedule
  state               = var.triggers_enabled ? "ENABLED" : "DISABLED"

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "stale_lock" {
  rule = aws_cloudwatch_event_rule.stale_lock.name
  arn  = aws_lambda_function.stale_lock_check.arn
}

resource "aws_lambda_permission" "eventbridge_stale_lock" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.stale_lock_check.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.stale_lock.arn
}

# ════════════════════════════════════════════════════════════════
#  Dropzone Freshness Check Lambda  (PLAN.md §14 alert #6)
#  Verifies upstream Data.ai dropped today's files into the bucket.
# ════════════════════════════════════════════════════════════════

resource "aws_iam_role" "dropzone_freshness_lambda" {
  name = "iodp-dc-dropzone-freshness-lambda-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "dropzone_freshness_lambda" {
  name = "iodp-dc-dropzone-freshness-lambda-policy-${var.environment}"
  role = aws_iam_role.dropzone_freshness_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.dropzone_bucket_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.alerts.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:*:*:*"]
      },
    ]
  })
}

data "archive_file" "dropzone_freshness" {
  type        = "zip"
  source_dir  = "${path.module}/../../../lambda/dropzone_freshness_check"
  output_path = "${path.module}/lambda_dropzone_freshness.zip"
}

resource "aws_lambda_function" "dropzone_freshness_check" {
  function_name    = "iodp-dc-dropzone-freshness-${var.environment}"
  filename         = data.archive_file.dropzone_freshness.output_path
  source_code_hash = data.archive_file.dropzone_freshness.output_base64sha256
  handler          = "handler.handler"
  runtime          = "python3.12"
  timeout          = 60
  memory_size      = 256
  role             = aws_iam_role.dropzone_freshness_lambda.arn

  reserved_concurrent_executions = 1

  environment {
    variables = {
      DROPZONE_BUCKET        = var.dropzone_bucket_name
      DROPZONE_PREFIX        = "download_channel/"
      EXPECTED_STORES        = join(",", var.expected_dropzone_stores)
      CHECK_DATE_OFFSET_DAYS = "0"
      SNS_TOPIC_ARN          = aws_sns_topic.alerts.arn
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "dropzone_freshness" {
  name              = "/aws/lambda/iodp-dc-dropzone-freshness-${var.environment}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_cloudwatch_event_rule" "dropzone_freshness" {
  name                = "iodp-dc-dropzone-freshness-${var.environment}"
  description         = "Daily check that upstream Data.ai dropped expected files"
  schedule_expression = var.dropzone_freshness_schedule
  state               = var.triggers_enabled ? "ENABLED" : "DISABLED"

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "dropzone_freshness" {
  rule = aws_cloudwatch_event_rule.dropzone_freshness.name
  arn  = aws_lambda_function.dropzone_freshness_check.arn
}

resource "aws_lambda_permission" "eventbridge_dropzone_freshness" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dropzone_freshness_check.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.dropzone_freshness.arn
}

# ════════════════════════════════════════════════════════════════
#  CloudWatch Dashboard
# ════════════════════════════════════════════════════════════════

resource "aws_cloudwatch_dashboard" "dc_etl" {
  dashboard_name = "iodp-dc-etl-${var.environment}"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Glue Job Duration (seconds)"
          metrics = [for name in var.glue_job_names : ["Glue", "glue.driver.aggregate.elapsedTime", "JobName", name, "Type", "gauge"]]
          period  = 86400
          stat    = "Average"
          region  = var.aws_region
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Glue Job Failures"
          metrics = [for name in var.glue_job_names : ["Glue", "glue.driver.aggregate.numFailedTasks", "JobName", name, "Type", "gauge"]]
          period  = 86400
          stat    = "Sum"
          region  = var.aws_region
        }
      },
    ]
  })
}
