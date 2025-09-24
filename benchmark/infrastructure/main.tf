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

# Generate random suffix for all resources (S3 bucket and instance resources)
resource "random_string" "suffix" {
  length  = 8
  special = false
  upper   = false
}

# Create S3 bucket for Embucket data with randomized suffix
resource "aws_s3_bucket" "embucket_benchmark" {
  bucket        = "embucket-benchmark-${var.aws_region}-${random_string.suffix.result}"
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

# Create key pair for EC2 access
resource "aws_key_pair" "embucket_benchmark_key" {
  key_name   = "${var.instance_name}-${random_string.suffix.result}-key"
  public_key = file("${var.public_key_path}")
}

# Create security group for EC2 instance
resource "aws_security_group" "embucket_benchmark_sg" {
  name        = "${var.instance_name}-${random_string.suffix.result}-sg"
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
    Name        = "${var.instance_name}-${random_string.suffix.result}-sg"
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
    Name        = "${var.instance_name}-${random_string.suffix.result}"
    Environment = var.environment
    Project     = "embucket-benchmark"
  }
}



# Generate .env file locally with existing AWS user credentials
resource "local_file" "env_file" {
  content = templatefile("${path.module}/env.tpl", {
    aws_access_key_id     = var.benchmark_s3_user_key_id
    aws_secret_access_key = var.benchmark_s3_user_access_key
    s3_bucket            = aws_s3_bucket.embucket_benchmark.bucket
    aws_region           = var.aws_region
    cors_allow_origin    = "http://${aws_instance.embucket_benchmark.public_ip}:8080"
    catalog_url          = "http://${aws_instance.embucket_benchmark.public_ip}:3000"
    server_address       = "http://${aws_instance.embucket_benchmark.public_ip}:3000"
    vite_api_url         = "http://${aws_instance.embucket_benchmark.public_ip}:3000"
    instance_public_ip   = aws_instance.embucket_benchmark.public_ip
    private_key_path     = var.private_key_path
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
