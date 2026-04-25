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

  topic {
    topic_arn     = aws_sns_topic.silver_notifications.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "download_channel/"
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
