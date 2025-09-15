# Embucket Benchmarking Infrastructure

This directory contains Terraform configuration to set up AWS infrastructure for benchmarking Embucket performance. The infrastructure includes an EC2 instance, S3 bucket, and automated Embucket installation with database initialization.

## Team-Friendly Design

Each deployment creates unique resources with the same randomized suffix to prevent conflicts when multiple team members deploy infrastructure:

- **S3 Bucket**: `embucket-benchmark-us-east-2-a1b2c3d4`
- **EC2 Instance**: `embucket-benchmark-a1b2c3d4`
- **Security Group**: `embucket-benchmark-a1b2c3d4-sg`
- **Key Pair**: `embucket-benchmark-a1b2c3d4-key`

All resources share the same 8-character random suffix (e.g., `a1b2c3d4`), making it easy to identify which resources belong to the same deployment. This allows multiple team members to deploy independent benchmark environments using shared AWS credentials without resource naming conflicts.

## Quick Start

1. **Prerequisites**
   - AWS CLI configured with appropriate credentials (PowerUser permissions sufficient)
   - Terraform installed (>= 1.0)
   - SSH key pair for EC2 access

2. **Configure Variables**
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars with your AWS credentials and settings
   ```

   **Required variables to set:**
   - `benchmark_s3_user_key_id` - Your AWS Access Key ID
   - `benchmark_s3_user_access_key` - Your AWS Secret Access Key
   - `private_key_path` / `public_key_path` - SSH key paths

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
- **EC2 Instance**: Runs Embucket and benchmarking tools
- **S3 Bucket**: Stores Embucket data with versioning and encryption (randomized name for team collaboration)
- **Security Group**: Allows SSH and Embucket ports (22, 3000, 8080)
- **Key Pair**: For SSH access to the instance

### Authentication Method (PowerUser Compatible)

This infrastructure uses **existing AWS user credentials** instead of creating new IAM resources. This approach:

- ✅ **Works with PowerUser permissions** (doesn't require IAM user/role creation privileges)
- ✅ **Team-friendly** - multiple developers can use shared credentials with prefix-based S3 bucket access
- ✅ **Simple setup** - uses your existing AWS user credentials
- ✅ **Independent deployments** - each deployment gets a unique S3 bucket name

**How it works:**
1. You provide existing AWS user credentials in `terraform.tfvars`
2. Terraform creates an S3 bucket with randomized name (e.g., `embucket-benchmark-us-east-2-a1b2c3d4`)
3. Your AWS user policy should allow access to `embucket-benchmark-*` buckets
4. Embucket uses these credentials directly to access the specific S3 bucket

### Required AWS Policy

Your AWS user needs a policy that allows access to S3 buckets with the `embucket-benchmark-` prefix:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::embucket-benchmark-*/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::embucket-benchmark-*"
        }
    ]
}
```

This policy allows multiple team members to use the same credentials while each deployment creates its own unique bucket.

### Team Collaboration

- **Shared Credentials**: All team members can use the same AWS user credentials
- **Independent Buckets**: Each `terraform apply` creates a unique S3 bucket (e.g., `embucket-benchmark-us-east-2-a1b2c3d4`)
- **No Conflicts**: Random suffixes prevent bucket naming conflicts
- **Easy Cleanup**: Each developer can `terraform destroy` their own infrastructure independently

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
- `terraform.tfvars.example` - Example configuration (copy to `terraform.tfvars`)
- `user_data.sh` - EC2 initialization script
- `bootstrap.sh` - Embucket installation and startup script
- `validate.sh` - Deployment validation script
- `docker-compose.yml` - Embucket container configuration
- `env.tpl` - Environment template for Embucket configuration
- `setup_credentials.sh.tpl` - Template for manual credential setup script

## Troubleshooting

### Common Issues

1. **Instance not accessible**: Check security group rules and key pair
2. **Embucket not starting**: Check Docker logs with `docker-compose logs`
3. **Database initialization failed**: Verify S3 credentials and permissions
4. **S3 access denied**: Ensure your AWS user has the required S3 policy (see above)
5. **Empty credentials**: Make sure you've set `benchmark_s3_user_key_id` and `benchmark_s3_user_access_key` in `terraform.tfvars`
6. **Performance issues**: Consider upgrading instance type

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

# If credentials weren't provided during deployment, you can set them up manually:
./setup_credentials.sh
```

### Manual Credential Setup

If you didn't provide AWS credentials in `terraform.tfvars`, you can set them up after deployment:

1. SSH to the instance: `$(terraform output -raw ssh_command)`
2. Run the credential setup script: `./setup_credentials.sh`
3. Follow the prompts to enter your AWS credentials
4. Start Embucket: `docker-compose up -d`
