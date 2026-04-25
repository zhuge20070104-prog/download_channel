# modules/dynamodb/outputs.tf

output "checkpoint_table_name" {
  value = aws_dynamodb_table.checkpoint.name
}

output "checkpoint_table_arn" {
  value = aws_dynamodb_table.checkpoint.arn
}
