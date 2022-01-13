data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

resource "aws_instance" "fb_cluster_nodes" {
  count                  = var.fb_data_node_count
  ami                    = data.aws_ami.amazon_linux_2.id
  instance_type          = var.fb_data_node_type
  key_name               = aws_key_pair.gitlab-featurebase-ci.key_name
  vpc_security_group_ids = [aws_security_group.featurebase.id]
  monitoring             = true
  subnet_id              = var.subnet != "" ? var.subnet : var.vpc_private_subnets[count.index % length(var.vpc_private_subnets)]
  availability_zone      = var.zone != "" ? var.zone : var.azs[count.index % length(var.azs)]
  iam_instance_profile   = "${aws_iam_instance_profile.fb_cluster_node_profile.name}"

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
  }

  ebs_block_device {
    device_name = "/dev/sdb"
    volume_type = var.fb_data_disk_type
    volume_size = var.fb_data_disk_size_gb
    iops        = var.fb_data_disk_iops
  }

  tags = {
    Prefix = "${var.cluster_prefix}"
    Name   = "${var.cluster_prefix}-featurebase-cluster-${count.index}"
    Role   = "cluster_node"
  }

}

resource "aws_instance" "fb_ingest" {
  count                       = var.fb_ingest_node_count
  ami                         = data.aws_ami.amazon_linux_2.id
  key_name                    = aws_key_pair.gitlab-featurebase-ci.key_name
  vpc_security_group_ids      = [aws_security_group.ingest.id]
  instance_type               = var.fb_ingest_type
  associate_public_ip_address = true
  monitoring                  = true
  subnet_id                   = var.subnet != "" ? var.subnet : var.vpc_public_subnets[count.index % length(var.vpc_public_subnets)]
  availability_zone           = var.zone != "" ? var.zone : var.azs[count.index % length(var.azs)]
  iam_instance_profile        = "${aws_iam_instance_profile.fb_cluster_node_profile.name}"

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
  }

  ebs_block_device {
    device_name = "/dev/sdb"
    volume_type = var.fb_ingest_disk_type
    volume_size = var.fb_ingest_disk_size_gb
    iops        = var.fb_ingest_disk_iops
  }

  tags = {
    Prefix = "${var.cluster_prefix}"
    Name   = "${var.cluster_prefix}-featurebase-ingest-${count.index}"
    Role   = "ingest_node"
  }

}

resource "aws_key_pair" "gitlab-featurebase-ci" {
  key_name   = "${var.cluster_prefix}-gitlab-ci"
  public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC91hhpVHNonAG7ku2ugpxEskf9KHeyHJPQJT26OHrMUw7R+T5A8TjqSzTau07sXQ/E9SO3ebV8SJ5PqeaQOnQB8VEvVNK0DjQH7ppvNg1Rfs42FZT9ttzTMvOjsSbK3vZTHXdoKQEdC9NxBwSkFIRGQojK1HUOq9xGrw31fA1OjSwlpLcbx7yyg18lcqW6UOptnVR8U9Yy9qQ5jZF1HtkQ6L9J+gv4o1UyNAUK2bopeGiXpBc3PQ/CFaFT2h/aqLBP66qAHsHVyAFD3PIRtplC5EHa8jXDgLacEls0uF7Q3kRPxvzcuo4g4VkOn1rDy9qH3vd2hT3aKVnM73FIDUiL"
  
  tags = {
    Prefix = "${var.cluster_prefix}"
    Name   = "${var.cluster_prefix}-gitlab-featurebase-ci"
    Role   = "ssh_keypair"
  }
}

resource "aws_security_group" "featurebase" {
  name        = "${var.cluster_prefix}-allow_featurebase"
  description = "Allow featurebase inbound traffic"
  vpc_id      = var.vpc_id

  ingress {
    description = "icmp from Anywhere"
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTP from Internal"
    from_port   = 10101
    to_port     = 10101
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8", "172.31.0.0/16"]
  }

  ingress {
    description = "GRPC from Internal"
    from_port   = 20101
    to_port     = 20101
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8", "172.31.0.0/16"]
  }

  ingress {
    description = "PostgreSQL from Internal"
    from_port   = 55432
    to_port     = 55432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8", "172.31.0.0/16"]
  }

  ingress {
    description = "etcd from internal"
    from_port   = 10301
    to_port     = 10301
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  ingress {
    description = "etcd from internal 2"
    from_port   = 10401
    to_port     = 10401
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  ingress {
    description      = "SSH"
    from_port        = 22
    to_port          = 22
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Prefix = "${var.cluster_prefix}"
    Name   = "${var.cluster_prefix}-allow_featurebase"
    Role   = "allow_featurebase"
  }
}

resource "aws_security_group" "ingest" {
  name        = "${var.cluster_prefix}-allow_ingest"
  description = "Allow ingest inbound traffic"
  vpc_id      = var.vpc_id

  ingress {
    description = "icmp from Anywhere"
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port        = 10101
    to_port          = 10101
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  ingress {
    description      = "SSH"
    from_port        = 22
    to_port          = 22
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Prefix = "${var.cluster_prefix}"
    Name   = "${var.cluster_prefix}-allow_ingest"
    Role   = "allow_ingest"
  }
}

resource "aws_iam_instance_profile" "fb_cluster_node_profile" {
  name = "${var.cluster_prefix}-fb_cluster_node_profile"
  role = aws_iam_role.fb_cluster_node_role.name

  tags = {
    Prefix = "${var.cluster_prefix}"
    Name   = "${var.cluster_prefix}-fb_cluster_node_profile"
    Role   = "fb_cluster_node_profile"
  }
}

resource "aws_iam_role" "fb_cluster_node_role" {
  name = "${var.cluster_prefix}-fb_cluster_node"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })

  inline_policy {
    name = "ec2_read_all"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action   = ["ec2:Describe*"]
          Effect   = "Allow"
          Resource = "*"
        },
      ]
    })
  }

  tags = {
    Prefix = "${var.cluster_prefix}"
    Name   = "${var.cluster_prefix}-fb_cluster_node_role"
    Role   = "fb_cluster_node_role"
  }
}