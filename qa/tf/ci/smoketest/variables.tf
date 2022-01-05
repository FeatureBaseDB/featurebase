variable "region" {
  description = "The AWS region in which the VPC should be built"
  type        = string
}

variable "profile" {
  description = "The name of the AWS profile Terraform should use for auth."
  type        = string
}

variable "gitlab_token" {
  description = "The API token for taking to Gitlab API - expected to come from an env variable."
  type        = string
}

variable "cluster_prefix" {
  type        = string
  description = "This is a identifier that will be prefixed to created resources"
}