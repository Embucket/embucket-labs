output "instance_id" {
  description = "ID of the EC2 instance"
  value       = aws_instance.tpch_datagen.id
}

output "instance_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_instance.tpch_datagen.public_ip
}

output "instance_public_dns" {
  description = "Public DNS name of the EC2 instance"
  value       = aws_instance.tpch_datagen.public_dns
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket for TPC-H data"
  value       = local.s3_bucket_name
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket for TPC-H data"
  value       = local.s3_bucket_arn
}

output "s3_prefix" {
  description = "S3 prefix for TPC-H data"
  value       = var.s3_prefix
}

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh -i ${var.private_key_path} ec2-user@${aws_instance.tpch_datagen.public_ip}"
}

output "data_generation_command" {
  description = "Command to run data generation on the instance"
  value       = "ssh -i ${var.private_key_path} ec2-user@${aws_instance.tpch_datagen.public_ip} './generate_tpch_data.sh ${join(" ", var.scale_factors)} ${local.s3_bucket_name} \"${var.s3_prefix}\"'"
}
