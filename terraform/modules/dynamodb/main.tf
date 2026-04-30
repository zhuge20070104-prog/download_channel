# modules/dynamodb/main.tf

resource "aws_dynamodb_table" "checkpoint" {
  name         = "iodp-dc-checkpoint-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "partition_key"

  attribute {
    name = "partition_key"
    type = "S"
  }

  # status / lock_expires_at 只在 Job 跑动时存在；Job 成功或失败后被 REMOVE。
  # GSI 因此变成稀疏索引：只含正在跑（或上轮卡死）的少数几条。
  # 用途：stale-lock Lambda 直接 Query "running" 分区，不再 Scan 全表。
  attribute {
    name = "status"
    type = "S"
  }

  attribute {
    name = "lock_expires_at"
    type = "S"
  }

  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    range_key       = "lock_expires_at"
    projection_type = "INCLUDE"
    non_key_attributes = [
      "partition_key",
      "last_processed_at",
      "job_run_id",
    ]
  }

  ttl {
    attribute_name = "TTL"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = var.tags
}
