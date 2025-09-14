# Embucket Benchmarking Infrastructure

This directory contains Terraform configuration to set up AWS infrastructure for benchmarking Embucket performance. The infrastructure includes an EC2 instance, S3 bucket, and automated Embucket installation with database initialization.

## Quick Start

1. **Prerequisites**
   - AWS CLI configured with appropriate credentials (PowerUser permissions sufficient)
   - Terraform installed (>= 1.0)
   - SSH key pair for EC2 access

2. **Configure Variables**
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars with your settings
   ```

3. **Deploy Infrastructure**
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

4. **Validate Deployment**
   ```bash
   ./validate.sh
   ```

## EC2 Instance Configuration

### Default Instance Type
- **Default**: `c7i.4xlarge` (16 vCPU, 32 GB RAM)
- **Optimized for**: CPU-intensive benchmarking workloads
- **Network**: Up to 12.5 Gbps network performance

### Changing Instance Type

To use a different EC2 instance type, modify the `instance_type` variable in your `terraform.tfvars`:

```hcl
instance_type = "c7i.2xlarge"  # 8 vCPU, 16 GB RAM
# or
instance_type = "c7i.8xlarge"  # 32 vCPU, 64 GB RAM
# or
instance_type = "m7i.4xlarge"  # 16 vCPU, 64 GB RAM (memory optimized)
```

### Recommended Instance Types for Benchmarking

| Instance Type | vCPU | Memory | Network | Use Case |
|---------------|------|--------|---------|----------|
| `c7i.2xlarge` | 8    | 16 GB  | Up to 12.5 Gbps | Light benchmarking |
| `c7i.4xlarge` | 16   | 32 GB  | Up to 12.5 Gbps | **Default - Balanced** |
| `c7i.8xlarge` | 32   | 64 GB  | 12.5 Gbps | Heavy CPU workloads |
| `c7i.12xlarge`| 48   | 96 GB  | 18.75 Gbps | Very heavy workloads |
| `m7i.4xlarge` | 16   | 64 GB  | Up to 12.5 Gbps | Memory-intensive queries |
| `r7i.4xlarge` | 16   | 128 GB | Up to 12.5 Gbps | Large dataset benchmarks |

## Infrastructure Components

### AWS Resources Created
- **EC2 Instance**: Runs Embucket and benchmarking tools with IAM instance profile
- **S3 Bucket**: Stores Embucket data with versioning and encryption
- **IAM Roles & Policies**: Role-based access for S3 (PowerUser compatible)
- **Security Group**: Allows SSH and Embucket ports
- **Key Pair**: For SSH access to the instance

### IAM Role-Based Authentication

This infrastructure uses **IAM roles instead of IAM users** to provide AWS credentials to Embucket. This approach:

- ✅ **Works with PowerUser permissions** (doesn't require IAM user creation privileges)
- ✅ **More secure** - no long-lived access keys stored on disk
- ✅ **Automatic credential rotation** - temporary credentials expire and refresh automatically
- ✅ **Follows AWS best practices** for EC2-to-AWS service authentication

**How it works:**
1. EC2 instance has an IAM instance profile attached
2. Instance profile allows assuming a dedicated Embucket application role
3. Bootstrap script generates temporary credentials by assuming the role
4. Embucket uses these temporary credentials to access S3

### Embucket Configuration
- **API Port**: 3000
- **UI Port**: 8080
- **Storage**: S3 backend with automatic volume/database creation
- **Authentication**: Default user `embucket/embucket`

## Access Information

After deployment, use these commands to access your infrastructure:

```bash
# Get instance IP
terraform output instance_public_ip

# SSH to instance
terraform output ssh_command

# Access Embucket API
curl http://$(terraform output -raw instance_public_ip):3000/health

# Access Embucket UI
open http://$(terraform output -raw instance_public_ip):8080
```

## Validation

The deployment includes automatic validation that checks:
- ✅ EC2 instance is running
- ✅ Embucket API is responding (port 3000)
- ✅ Embucket UI is accessible (port 8080)
- ✅ Database initialization completed successfully
- ✅ S3 bucket is accessible

Run validation manually:
```bash
./validate.sh
```

## Benchmarking

Once deployed, you can:
1. SSH to the instance: `$(terraform output -raw ssh_command)`
2. Run custom benchmarks against the Embucket API
3. Monitor performance using AWS CloudWatch
4. Scale instance type up/down as needed

## Cleanup

To destroy all resources:
```bash
terraform destroy
```

## Configuration Files

- `main.tf` - Main Terraform configuration
- `variables.tf` - Input variables
- `outputs.tf` - Output values
- `terraform.tfvars.example` - Example configuration
- `user_data.sh` - EC2 initialization script
- `bootstrap.sh` - Embucket installation script
- `validate.sh` - Deployment validation script
- `docker-compose.yml` - Embucket container configuration
- `env.tpl` - Environment template

## Troubleshooting

### Common Issues

1. **Instance not accessible**: Check security group rules and key pair
2. **Embucket not starting**: Check Docker logs with `docker-compose logs`
3. **Database initialization failed**: Verify S3 credentials and permissions
4. **Performance issues**: Consider upgrading instance type

### Logs and Debugging

```bash
# SSH to instance
$(terraform output -raw ssh_command)

# Check Docker containers
docker-compose ps
docker-compose logs embucket
docker-compose logs db-init

# Check system resources
htop
df -h
```
