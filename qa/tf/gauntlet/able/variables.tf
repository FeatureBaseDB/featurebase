variable "region" {
  description = "The AWS region in which the VPC should be built"
  type        = string
}

variable "profile" {
  description = "The name of the AWS profile Terraform should use for auth."
  type        = string
}

variable "cluster_prefix" {
  type        = string
  description = "This is a identifier that will be prefixed to created resources"
}
