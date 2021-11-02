terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.27"
    }
  }

  required_version = ">= 1.0.0"
}

provider "aws" {
  region  = "us-east-2"
}

resource "aws_security_group" "server_sg" {
  name = "Load Balancer Security Group"

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = -1
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "cloudinit_config" "server_config" {
  gzip          = true
  base64_encode = true
  part {
    content_type = "text/cloud-config"
    content = templatefile("${path.module}/server.yml", {
      header : aws_security_group.server_sg.id
    })
  }
}

resource "aws_instance" "server_instance1" {
  ami           = var.ami_linux_amd
  instance_type = "t2.micro"

  key_name = "souhaila-rsa"

  vpc_security_group_ids      = [aws_security_group.server_sg.id]
  user_data                   = data.cloudinit_config.server_config.rendered
  associate_public_ip_address = true

  tags = {
    Name = var.instance_name
  }
}