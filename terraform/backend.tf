# terraform/backend.tf
# S3 remote state + DynamoDB lock

terraform {
  backend "s3" {
    bucket         = "iodp-terraform-state-prod"
    key            = "download-channel/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "iodp-terraform-locks"
    encrypt        = true
  }
}
