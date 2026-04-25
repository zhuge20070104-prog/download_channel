# modules/snowpipe/main.tf
#
# AWS-side resources for Snowpipe:
# - IAM role with Snowflake trust (bidirectional)
# - SQS queue for Snowpipe AUTO_INGEST
# - SNS subscription (Silver S3 events → SQS → Snowpipe)
#
# Note: The actual Snowflake Pipe/Stage/FileFormat objects are created via
# snowflake_sql/04_pipe.sql, not Terraform (provider limitations with AUTO_INGEST).

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
#  SQS Queue for Snowpipe AUTO_INGEST
# ════════════════════════════════════════════════════════════════

resource "aws_sqs_queue" "snowpipe" {
  name                       = "iodp-dc-snowpipe-queue-${var.environment}"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 10

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
