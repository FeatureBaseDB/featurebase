variable "instance_name" {
  description = "Value of the Name tag for the EC2 instance"
  type        = string
  default     = "single-node-deployment-1"
}

variable "ami_linux_amd" {
  type    = string
  default = "ami-0f19d220602031aed"
}
