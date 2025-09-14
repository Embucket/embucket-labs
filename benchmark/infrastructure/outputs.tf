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

output "credential_setup_script" {
  description = "Path to the credential setup script (PowerUser workaround)"
  value       = "${path.module}/setup_credentials.sh"
}

output "setup_status" {
  description = "Setup status and access information"
  value = var.benchmark_s3_user_key_id != "" && var.benchmark_s3_user_key_id != "AKIA_YOUR_ACCESS_KEY_ID_HERE" ? "✅ Setup Complete! Embucket is starting automatically. Access URLs: API: http://${aws_instance.embucket_benchmark.public_ip}:3000, UI: http://${aws_instance.embucket_benchmark.public_ip}:8080" : "⚠️ Please update terraform.tfvars with your actual AWS credentials and run terraform apply"
}

output "access_urls" {
  description = "Access URLs for Embucket"
  value = {
    api_url = "http://${aws_instance.embucket_benchmark.public_ip}:3000"
    ui_url  = "http://${aws_instance.embucket_benchmark.public_ip}:8080"
    ssh_command = "ssh -i ~/.ssh/id_rsa ec2-user@${aws_instance.embucket_benchmark.public_ip}"
  }
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
