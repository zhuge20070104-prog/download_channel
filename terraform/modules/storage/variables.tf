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
