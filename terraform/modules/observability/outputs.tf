# modules/observability/outputs.tf

output "sns_alert_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "sns_alert_topic_name" {
  value = aws_sns_topic.alerts.name
}

output "dlq_report_lambda_name" {
  value = aws_lambda_function.dlq_weekly_report.function_name
}

output "stale_lock_lambda_name" {
  value = aws_lambda_function.stale_lock_check.function_name
}

output "dropzone_freshness_lambda_name" {
  value = aws_lambda_function.dropzone_freshness_check.function_name
}

output "dashboard_name" {
  value = aws_cloudwatch_dashboard.dc_etl.dashboard_name
}
