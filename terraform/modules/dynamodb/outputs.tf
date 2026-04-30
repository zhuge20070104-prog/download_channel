# modules/dynamodb/outputs.tf

output "checkpoint_table_name" {
  value = aws_dynamodb_table.checkpoint.name
}

output "checkpoint_table_arn" {
  value = aws_dynamodb_table.checkpoint.arn
}

output "checkpoint_status_index_arn" {
  description = "ARN of the sparse status-index GSI (used by stale-lock Lambda Query)"
  value       = "${aws_dynamodb_table.checkpoint.arn}/index/status-index"
}
