# modules/storage/main.tf

locals {
  bronze_bucket_name  = "iodp-dc-bronze-${var.environment}-${var.aws_account_id}"
  silver_bucket_name  = "iodp-dc-silver-${var.environment}-${var.aws_account_id}"
  scripts_bucket_name = "iodp-dc-scripts-${var.environment}-${var.aws_account_id}"
}

# ════════════════════════════════════════════════════════════════
#  Bronze Bucket
# ════════════════════════════════════════════════════════════════

resource "aws_s3_bucket" "bronze" {
  bucket = local.bronze_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "bronze-standard-lifecycle"
    status = "Enabled"

    transition {
      days          = var.ia_transition_days
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = var.glacier_transition_days
      storage_class = "GLACIER_IR"
    }
    expiration {
      days = var.expiration_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "dead-letter-lifecycle"
    status = "Enabled"

    filter {
      prefix = var.dead_letter_prefix
    }

    expiration {
      days = 30
    }
  }
}

# ════════════════════════════════════════════════════════════════
#  Silver Bucket
# ════════════════════════════════════════════════════════════════

resource "aws_s3_bucket" "silver" {
  bucket = local.silver_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "silver-standard-lifecycle"
    status = "Enabled"

    transition {
      days          = var.ia_transition_days
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = var.glacier_transition_days
      storage_class = "GLACIER_IR"
    }
    expiration {
      days = var.expiration_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# SNS topic for S3 → Snowpipe notification
resource "aws_sns_topic" "silver_notifications" {
  name = "iodp-dc-silver-s3-notify-${var.environment}"
  tags = var.tags
}

resource "aws_sns_topic_policy" "silver_notifications" {
  arn = aws_sns_topic.silver_notifications.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowS3Publish"
        Effect    = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action    = "SNS:Publish"
        Resource  = aws_sns_topic.silver_notifications.arn
        Condition = {
          ArnLike = {
            "aws:SourceArn" = aws_s3_bucket.silver.arn
          }
        }
      }
    ]
  })
}

resource "aws_s3_bucket_notification" "silver" {
  bucket = aws_s3_bucket.silver.id

  # S3 bucket notification 直发 Snowflake 自管的 SQS（AUTO_INGEST 模式）：
  # S3 PUT → Snowflake SQS → Snowpipe COPY。SQS ARN 是动态值，由 Makefile
  # 的 deploy-infra-phase2 通过 scripts/get_pipe_sqs_arn.sh 提取并通过
  # -var=snowflake_pipe_sqs_arn=... 注入。
  #
  # 为什么不留 topic block 给自家 SNS 监控？AWS S3 不允许同一 event type 上
  # 两条 prefix 重叠的规则，topic 跟 queue 互斥。Snowpipe ingest 是关键路径，
  # 优先；自家 SNS+SQS+CloudWatch alarms 变成 dangling resource（不工作但
  # 也不报错）。Snowpipe-stuck 监控后续改用 Snowflake 端 freshness alert
  # 实现（snowflake_sql/08_freshness_alert.sql）。
  dynamic "queue" {
    for_each = var.snowflake_pipe_sqs_arn != "" ? [1] : []
    content {
      queue_arn     = var.snowflake_pipe_sqs_arn
      events        = ["s3:ObjectCreated:*"]
      filter_prefix = "download_channel/"
    }
  }

  depends_on = [aws_sns_topic_policy.silver_notifications]
}

# ════════════════════════════════════════════════════════════════
#  Scripts Bucket
# ════════════════════════════════════════════════════════════════

resource "aws_s3_bucket" "scripts" {
  bucket = local.scripts_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
