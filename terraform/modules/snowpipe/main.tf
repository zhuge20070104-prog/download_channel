# modules/snowpipe/main.tf
#
# AWS-side resources for Snowpipe:
# - IAM role with Snowflake trust (bidirectional)
# - SQS queue for Snowpipe AUTO_INGEST
# - SNS subscription (Silver S3 events → SQS → Snowpipe)
#
# Note: Snowflake-side resources (database, schemas, roles, grants, storage
# integration) live in terraform/modules/snowflake/. The Pipe/Stage/FileFormat
# objects are created via snowflake_sql/04_pipe.sql due to provider limitations
# with AUTO_INGEST.

# ════════════════════════════════════════════════════════════════
#  IAM Role — Snowflake assumes this to read Silver S3
# ════════════════════════════════════════════════════════════════

resource "aws_iam_role" "snowpipe_s3_access" {
  name = "iodp-dc-snowpipe-s3-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = var.snowflake_iam_user_arn }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.snowflake_external_id
          }
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "snowpipe_s3_read" {
  name = "iodp-dc-snowpipe-s3-read-${var.environment}"
  role = aws_iam_role.snowpipe_s3_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
        ]
        Resource = ["${var.silver_bucket_arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.silver_bucket_arn]
      },
    ]
  })
}

# ════════════════════════════════════════════════════════════════
#  SQS Queue for Snowpipe AUTO_INGEST + DLQ
# ════════════════════════════════════════════════════════════════
#
# DLQ scope note: Snowpipe deletes SQS messages after each COPY attempt
# regardless of success/failure (COPY-level errors are recorded in Snowflake's
# COPY_HISTORY view, NOT here). So this DLQ catches delivery-level failures
# only — i.e., Snowpipe receives a message but visibility timeout expires 5
# times before it can ack (Snowpipe slow / stuck / transient Snowflake errors).
# For COPY-level error visibility, query COPY_HISTORY in Snowflake.

resource "aws_sqs_queue" "snowpipe_dlq" {
  name                      = "iodp-dc-snowpipe-dlq-${var.environment}"
  message_retention_seconds = 1209600 # 14 days (SQS max) — give time to investigate before auto-drop

  tags = var.tags
}

resource "aws_sqs_queue" "snowpipe" {
  name                       = "iodp-dc-snowpipe-queue-${var.environment}"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 10

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.snowpipe_dlq.arn
    maxReceiveCount     = 5
  })

  tags = var.tags
}

resource "aws_sqs_queue_policy" "snowpipe" {
  queue_url = aws_sqs_queue.snowpipe.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowSNSPublish"
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.snowpipe.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = var.silver_sns_topic_arn
          }
        }
      }
    ]
  })
}

# ════════════════════════════════════════════════════════════════
#  SNS Subscription: Silver S3 events → SQS
# ════════════════════════════════════════════════════════════════

resource "aws_sns_topic_subscription" "snowpipe" {
  topic_arn = var.silver_sns_topic_arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.snowpipe.arn
}

# ════════════════════════════════════════════════════════════════
#  CloudWatch Alarms
# ════════════════════════════════════════════════════════════════

# Alarm 1: any message in the DLQ — Snowpipe failed to ack a message after
# 5 receives. Fires within ~5 min of the message landing in DLQ.
resource "aws_cloudwatch_metric_alarm" "snowpipe_dlq_has_messages" {
  alarm_name          = "iodp-dc-snowpipe-dlq-not-empty-${var.environment}"
  alarm_description   = "Snowpipe DLQ has messages — Snowpipe failed to ack S3 events after maxReceiveCount=5"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [var.sns_alert_topic_arn]
  ok_actions    = [var.sns_alert_topic_arn]

  dimensions = {
    QueueName = aws_sqs_queue.snowpipe_dlq.name
  }

  tags = var.tags
}

# Alarm 2: main queue oldest message age > 30 min sustained for 10 min.
# Catches the case where Snowpipe is NOT polling at all (IAM revoked /
# Snowflake outage / pipe disabled) — DLQ alone misses this because
# maxReceiveCount only increments on actual receives.
resource "aws_cloudwatch_metric_alarm" "snowpipe_queue_age" {
  alarm_name          = "iodp-dc-snowpipe-queue-stale-${var.environment}"
  alarm_description   = "Snowpipe main queue oldest message > 30 min — Snowpipe may not be polling"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 1800
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [var.sns_alert_topic_arn]

  dimensions = {
    QueueName = aws_sqs_queue.snowpipe.name
  }

  tags = var.tags
}
