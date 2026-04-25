# modules/snowflake/variables.tf

variable "environment" {
  type = string
}

variable "silver_bucket_name" {
  type = string
}

variable "snowpipe_iam_role_arn" {
  description = "AWS IAM role ARN for Snowflake to assume when reading S3. Empty string on first deploy."
  type        = string
  default     = ""
}
