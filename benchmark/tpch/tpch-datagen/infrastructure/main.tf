terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.1"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.1"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

# Generate random suffix for all resources
resource "random_string" "suffix" {
  length  = 8
  special = false
  upper   = false
}

# Use existing S3 bucket
locals {
  s3_bucket_name = var.existing_s3_bucket
  s3_bucket_arn  = "arn:aws:s3:::${var.existing_s3_bucket}"
}

# IAM policy for S3 access
resource "aws_iam_policy" "tpch_s3_policy" {
  name        = "tpch-datagen-s3-policy-${random_string.suffix.result}"
  description = "Policy for TPC-H data generation S3 access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          local.s3_bucket_arn,
          "${local.s3_bucket_arn}/*"
        ]
      }
    ]
  })
}

# IAM role for EC2 instance
resource "aws_iam_role" "tpch_datagen_role" {
  name = "tpch-datagen-role-${random_string.suffix.result}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "tpch_s3_policy_attachment" {
  role       = aws_iam_role.tpch_datagen_role.name
  policy_arn = aws_iam_policy.tpch_s3_policy.arn
}

resource "aws_iam_instance_profile" "tpch_datagen_profile" {
  name = "tpch-datagen-profile-${random_string.suffix.result}"
  role = aws_iam_role.tpch_datagen_role.name
}

# Security group for SSH access
resource "aws_security_group" "tpch_datagen_sg" {
  name        = "tpch-datagen-sg-${random_string.suffix.result}"
  description = "Security group for TPC-H data generation instance"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "tpch-datagen-sg-${random_string.suffix.result}"
  }
}

# Key pair for SSH access
resource "aws_key_pair" "tpch_datagen_key" {
  key_name   = "tpch-datagen-key-${random_string.suffix.result}"
  public_key = file(var.public_key_path)
}

# Get latest Amazon Linux 2023 AMI
data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Create EC2 instance for data generation
resource "aws_instance" "tpch_datagen" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.instance_type
  key_name              = aws_key_pair.tpch_datagen_key.key_name
  vpc_security_group_ids = [aws_security_group.tpch_datagen_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.tpch_datagen_profile.name

  user_data = file("${path.module}/user_data.sh")

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_size
    encrypted   = true
    iops        = var.root_volume_iops
    throughput  = var.root_volume_throughput
  }

  tags = {
    Name        = "tpch-datagen-${random_string.suffix.result}"
    Environment = var.environment
    Project     = "tpch-datagen"
  }
}

# Wait for instance to be ready
resource "null_resource" "wait_for_instance" {
  depends_on = [aws_instance.tpch_datagen]

  connection {
    type        = "ssh"
    user        = "ec2-user"
    private_key = file(var.private_key_path)
    host        = aws_instance.tpch_datagen.public_ip
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = [
      "echo 'Instance is ready'"
    ]
  }
}

# Upload data generation script
resource "null_resource" "upload_scripts" {
  depends_on = [null_resource.wait_for_instance]

  connection {
    type        = "ssh"
    user        = "ec2-user"
    private_key = file(var.private_key_path)
    host        = aws_instance.tpch_datagen.public_ip
  }

  # Upload data generation script
  provisioner "file" {
    source      = "${path.module}/generate_tpch_data.sh"
    destination = "/home/ec2-user/generate_tpch_data.sh"
  }

  # Make script executable
  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/ec2-user/generate_tpch_data.sh"
    ]
  }
}
