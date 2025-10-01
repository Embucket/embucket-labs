variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-2"
}

variable "aws_profile" {
  description = "AWS profile to use (optional)"
  type        = string
  default     = null
}

variable "instance_type" {
  description = "EC2 instance type for data generation"
  type        = string
  default     = "c7i.8xlarge"
}

variable "root_volume_size" {
  description = "Size of the root EBS volume in GB"
  type        = number
  default     = 2000
}

variable "root_volume_iops" {
  description = "IOPS for the root EBS volume"
  type        = number
  default     = 3000  # Good IOPS for SF=100
}

variable "root_volume_throughput" {
  description = "Throughput for the root EBS volume in MB/s"
  type        = number
  default     = 250  # Good throughput for SF=100
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "tpch-datagen"
}

variable "public_key_path" {
  description = "Path to the public key file for SSH access"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "private_key_path" {
  description = "Path to the private key file for SSH access"
  type        = string
  default     = "~/.ssh/id_rsa"
}

variable "scale_factors" {
  description = "List of TPC-H scale factors to generate"
  type        = list(number)
  default     = [1000]
}

variable "existing_s3_bucket" {
  description = "Existing S3 bucket name for TPC-H data storage"
  type        = string
  default     = "embucket-testdata"
}

variable "s3_prefix" {
  description = "S3 prefix/folder for TPC-H data"
  type        = string
  default     = "tpch"
}
