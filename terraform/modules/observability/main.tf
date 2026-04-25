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
#  Glue Job Failure Alarms
# ════════════════════════════════════════════════════════════════

resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  for_each = toset(var.glue_job_names)

  alarm_name          = "iodp-dc-${each.value}-failure-${var.environment}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Glue job ${each.value} has failed tasks"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    JobName = each.value
    Type    = "gauge"
  }

  tags = var.tags
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
          region  = "us-east-1"
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
          region  = "us-east-1"
        }
      },
    ]
  })
}
