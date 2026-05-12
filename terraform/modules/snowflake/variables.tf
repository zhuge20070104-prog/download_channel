# modules/snowflake/variables.tf

variable "environment" {
  type = string
}

variable "silver_bucket_name" {
  type = string
}

variable "snowpipe_iam_role_arn" {
  description = "AWS IAM role ARN for Snowflake to assume when reading S3. Caller must pass a predictable ARN to break the snowflake↔snowpipe circular dependency (see terraform/main.tf)."
  type        = string
}

