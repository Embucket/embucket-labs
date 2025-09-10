variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-2"
}

variable "instance_name" {
  description = "Name for the EC2 instance"
  type        = string
  default     = "embucket-benchmark"
}

variable "instance_type" {
  description = "EC2 instance type for benchmarking"
  type        = string
  default     = "c7i.4xlarge"  # 16 vCPU, 32 GB RAM - optimized for compute
}

variable "root_volume_size" {
  description = "Size of the root EBS volume in GB"
  type        = number
  default     = 100
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "benchmark"
}

variable "private_key_path" {
  description = "Path to the private key file for SSH access"
  type        = string
  default     = "~/.ssh/id_rsa"
}

variable "public_key_path" {
  description = "Path to the public key file for SSH access"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "aws_profile" {
  description = "AWS profile to use for authentication"
  type        = string
  default     = null
}
