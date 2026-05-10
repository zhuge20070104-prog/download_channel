# modules/snowpipe/outputs.tf

output "snowpipe_iam_role_arn" {
  value = aws_iam_role.snowpipe_s3_access.arn
}

output "snowpipe_sqs_arn" {
  value = aws_sqs_queue.snowpipe.arn
}

output "snowpipe_sqs_url" {
  value = aws_sqs_queue.snowpipe.url
}

output "snowpipe_dlq_arn" {
  value = aws_sqs_queue.snowpipe_dlq.arn
}

output "snowpipe_dlq_url" {
  value = aws_sqs_queue.snowpipe_dlq.url
}
