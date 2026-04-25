# modules/dynamodb/main.tf

resource "aws_dynamodb_table" "checkpoint" {
  name         = "iodp-dc-checkpoint-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "partition_key"

  attribute {
    name = "partition_key"
    type = "S"
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
