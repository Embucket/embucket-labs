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

# Generate random suffix for S3 bucket
resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Create S3 bucket for Embucket data
resource "aws_s3_bucket" "embucket_benchmark" {
  bucket        = "embucket-benchmark-${random_string.bucket_suffix.result}"
  force_destroy = true  # Allow deletion even if bucket contains objects
}

resource "aws_s3_bucket_versioning" "embucket_benchmark_versioning" {
  bucket = aws_s3_bucket.embucket_benchmark.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "embucket_benchmark_encryption" {
  bucket = aws_s3_bucket.embucket_benchmark.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "embucket_benchmark_pab" {
  bucket = aws_s3_bucket.embucket_benchmark.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create IAM user for Embucket
resource "aws_iam_user" "embucket_benchmark_user" {
  name = "embucket-benchmark-user"
  path = "/"
}

# Create access key for the IAM user
resource "aws_iam_access_key" "embucket_benchmark_user_key" {
  user = aws_iam_user.embucket_benchmark_user.name
}

# Create IAM policy for S3 bucket access
resource "aws_iam_policy" "embucket_benchmark_s3_policy" {
  name        = "embucket-benchmark-s3-access"
  description = "Policy for Embucket benchmark to access S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.embucket_benchmark.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.embucket_benchmark.arn
      }
    ]
  })
}

# Attach policy to user
resource "aws_iam_user_policy_attachment" "embucket_benchmark_user_policy_attachment" {
  user       = aws_iam_user.embucket_benchmark_user.name
  policy_arn = aws_iam_policy.embucket_benchmark_s3_policy.arn
}

# Create key pair for EC2 access
resource "aws_key_pair" "embucket_benchmark_key" {
  key_name   = "${var.instance_name}-key"
  public_key = file("${var.public_key_path}")
}

# Create security group for EC2 instance
resource "aws_security_group" "embucket_benchmark_sg" {
  name        = "${var.instance_name}-sg"
  description = "Security group for Embucket benchmark instance"

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Embucket API
  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Embucket UI
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # All outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.instance_name}-sg"
    Environment = var.environment
    Project     = "embucket-benchmark"
  }
}

# Get the latest Amazon Linux 2023 AMI
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

# Create EC2 instance
resource "aws_instance" "embucket_benchmark" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.instance_type
  key_name              = aws_key_pair.embucket_benchmark_key.key_name
  vpc_security_group_ids = [aws_security_group.embucket_benchmark_sg.id]
  
  user_data = file("${path.module}/user_data.sh")

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_size
    encrypted   = true
  }

  tags = {
    Name        = var.instance_name
    Environment = var.environment
    Project     = "embucket-benchmark"
  }
}

# Generate .env file locally
resource "local_file" "env_file" {
  content = templatefile("${path.module}/env.tpl", {
    aws_access_key_id     = aws_iam_access_key.embucket_benchmark_user_key.id
    aws_secret_access_key = aws_iam_access_key.embucket_benchmark_user_key.secret
    s3_bucket            = aws_s3_bucket.embucket_benchmark.bucket
    aws_region           = var.aws_region
    cors_allow_origin    = "http://${aws_instance.embucket_benchmark.public_ip}:8080"
    catalog_url          = "http://${aws_instance.embucket_benchmark.public_ip}:3000"
    server_address       = "http://${aws_instance.embucket_benchmark.public_ip}:3000"
    vite_api_url         = "http://${aws_instance.embucket_benchmark.public_ip}:3000"
  })
  filename = "${path.module}/.env"
}

# Wait for instance to be ready
resource "null_resource" "wait_for_instance" {
  depends_on = [aws_instance.embucket_benchmark]

  provisioner "local-exec" {
    command = "sleep 60"
  }
}

# Upload files to the instance
resource "null_resource" "upload_files" {
  depends_on = [null_resource.wait_for_instance, local_file.env_file]

  connection {
    type        = "ssh"
    user        = "ec2-user"
    private_key = file(var.private_key_path)
    host        = aws_instance.embucket_benchmark.public_ip
  }

  # Upload docker-compose.yml
  provisioner "file" {
    source      = "${path.module}/docker-compose.yml"
    destination = "/home/ec2-user/docker-compose.yml"
  }

  # Upload .env file
  provisioner "file" {
    source      = "${path.module}/.env"
    destination = "/home/ec2-user/.env"
  }

  # Create docker directory and upload files
  provisioner "remote-exec" {
    inline = [
      "mkdir -p /home/ec2-user/docker"
    ]
  }

  # Upload docker directory contents
  provisioner "file" {
    source      = "${path.module}/docker/"
    destination = "/home/ec2-user/docker"
  }

  # Upload bootstrap script
  provisioner "file" {
    source      = "${path.module}/bootstrap.sh"
    destination = "/home/ec2-user/bootstrap.sh"
  }

  # Run bootstrap script
  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/ec2-user/bootstrap.sh",
      "sudo /home/ec2-user/bootstrap.sh"
    ]
  }
}
