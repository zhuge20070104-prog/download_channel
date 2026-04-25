# modules/glue_dlq_replay/outputs.tf

output "dlq_replay_job_name" {
  value = aws_glue_job.dlq_replay.name
}
