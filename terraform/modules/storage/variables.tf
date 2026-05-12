# modules/storage/variables.tf

variable "environment" {
  type = string
}

variable "aws_account_id" {
  type = string
}

variable "dead_letter_prefix" {
  type    = string
  default = "dead_letter/"
}

variable "ia_transition_days" {
  type    = number
  default = 30
}

variable "glacier_transition_days" {
  type    = number
  default = 90
}

variable "expiration_days" {
  type    = number
  default = 365
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "snowflake_pipe_sqs_arn" {
  description = <<-EOT
    Snowflake-managed SQS ARN for Snowpipe AUTO_INGEST (e.g.
    arn:aws:sqs:ap-southeast-1:782091841703:sf-snowpipe-...). Dynamic per pipe;
    auto-populated by Makefile deploy-infra-phase2 via scripts/get_pipe_sqs_arn.sh.
    When empty (first deploy, before 04_pipe.sql has run), no queue block is
    added — the next deploy after pipe creation wires it.
  EOT
  type        = string
  default     = ""
}
