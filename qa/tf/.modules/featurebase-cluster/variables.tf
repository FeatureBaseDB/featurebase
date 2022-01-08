variable "cluster_prefix" {
  type        = string
  description = "This is a identifier that will be prefixed to created resources"
}

variable "fb_ingest_type" {
  type    = string
  default = "c6g.2xlarge"
}

variable "fb_ingest_node_count" {
  type    = number
  default = 1
}

variable "fb_data_node_type" {
  type    = string
  default = "c6g.16xlarge"
}

variable "fb_data_node_count" {
  type    = number
  default = 3
}

variable "fb_cluster_replica_count" {
  type    = number
  default = 1
}

variable "subnet" {
  default = ""
}

variable "zone" {
  default = ""
}

variable "fb_data_disk_type" {
  default = "gp3"
}
variable "fb_data_disk_iops" {
  default = 1000
}

variable "fb_data_disk_size_gb" {
  default = 100
}

variable "fb_ingest_disk_type" {
  default = "gp3"
}
variable "fb_ingest_disk_iops" {
  default = 1000
}

variable "fb_ingest_disk_size_gb" {
  default = 100
}

variable "azs" {
  type    = list(any)
  default = ["us-east-2a", "us-east-2b", "us-east-2c"]
}

variable "private_subnets" {
  type    = list(any)
  default = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "public_subnets" {
  type    = list(any)
  default = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

variable "vpc_cidr" {
  default = "10.0.0.0/16"
}

variable "region" {
  description = "Region to create AWS resources in"
  type        = string
}

variable "profile" {
  description = "Profile to use to authenticate with AWS"
  type        = string
}


variable "vpc_id" {
  description = "The VPC in which we will build the cluster"
  type        = string
}

variable "vpc_cidr_block" {
  description = "A delicious crisp cider associated with the VPC in which we will build the cluster"
  type        = string
}

variable "vpc_public_subnets" {
  description = "A public net underneath in the VPC in which we will build the cluster"
  type        = list(string)
}

variable "vpc_private_subnets" {
  description = "A private net underneath in the VPC in which we will build the cluster"
  type        = list(string)
}
