output "instance_id" {
  description = "ID of the EC2 instance"
  value       = aws_instance.embucket_benchmark.id
}

output "instance_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_instance.embucket_benchmark.public_ip
}

output "instance_public_dns" {
  description = "Public DNS name of the EC2 instance"
  value       = aws_instance.embucket_benchmark.public_dns
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket for Embucket data"
  value       = aws_s3_bucket.embucket_benchmark.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket for Embucket data"
  value       = aws_s3_bucket.embucket_benchmark.arn
}

output "iam_user_name" {
  description = "Name of the IAM user for Embucket"
  value       = aws_iam_user.embucket_benchmark_user.name
}

output "iam_access_key_id" {
  description = "Access key ID for the IAM user"
  value       = aws_iam_access_key.embucket_benchmark_user_key.id
  sensitive   = true
}

output "iam_secret_access_key" {
  description = "Secret access key for the IAM user"
  value       = aws_iam_access_key.embucket_benchmark_user_key.secret
  sensitive   = true
}

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh -i ${var.private_key_path} ec2-user@${aws_instance.embucket_benchmark.public_ip}"
}

output "embucket_api_url" {
  description = "URL for Embucket API"
  value       = "http://${aws_instance.embucket_benchmark.public_ip}:3000"
}

output "embucket_ui_url" {
  description = "URL for Embucket UI"
  value       = "http://${aws_instance.embucket_benchmark.public_ip}:8080"
}

output "instance_type" {
  description = "EC2 instance type used"
  value       = var.instance_type
}

output "aws_region" {
  description = "AWS region used"
  value       = var.aws_region
}
