# TPC-H Data Generation on AWS EC2

This infrastructure automatically generates TPC-H datasets at scale factor 100 using AWS EC2 and DuckDB, then uploads the data to your existing S3 bucket for use in benchmarking.

## Overview

The solution provides:
- **Automated EC2 deployment** with instance optimized for SF=100
- **DuckDB-based data generation** using the official TPC-H extension
- **Automatic S3 upload** to your existing `embucket-testdata` bucket
- **Cost optimization** through ephemeral infrastructure and right-sized instances
- **Simple single-scale-factor generation** focused on SF=100

## Instance Sizing Recommendations

### Scale Factor Guidelines

| Scale Factor | Data Size | Instance Type | Storage | Generation Time |
|--------------|-----------|---------------|---------|-----------------|
| SF=100       | ~100GB    | c7i.4xlarge   | 300GB   | ~1-2 hours      |

### Instance Type Details

- **c7i.4xlarge** (16 vCPU, 32GB RAM): Optimized for SF=100 (default)
- **c7i.2xlarge** (8 vCPU, 16GB RAM): Alternative for cost savings
- **c7i.8xlarge** (32 vCPU, 64GB RAM): Alternative for faster generation

### Storage Configuration

- **Moderate IOPS** (3000): Good performance for SF=100
- **Good Throughput** (250 MB/s): Efficient S3 uploads
- **GP3 volumes**: Cost-effective with configurable performance

## Quick Start

### 1. Prerequisites

- AWS CLI configured with appropriate permissions
- Terraform installed
- SSH key pair generated (`ssh-keygen -t rsa -b 4096`)

### 2. Configure Infrastructure

```bash
cd tpch-datagen/infrastructure
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your settings
```

### 3. Deploy Infrastructure

```bash
terraform init
terraform plan
terraform apply
```

### 4. Generate Data

The infrastructure will automatically upload the generation script. You can either:

**Option A: Use Terraform output command**
```bash
# Get the command from Terraform output
terraform output data_generation_command
# Run the returned command
```

**Option B: SSH manually**
```bash
# SSH to the instance
terraform output ssh_command

# Run with Terraform-configured settings
./generate_tpch_data.sh 100 1000 $(terraform output -raw s3_bucket_name) "$(terraform output -raw s3_prefix)"

# Or run with custom settings
./generate_tpch_data.sh 100 1000 my-custom-bucket "custom/path"
./generate_tpch_data.sh 50 100 my-bucket ""  # Empty prefix
```

**Option C: Custom bucket and folder**
```bash
# SSH to the instance
ssh -i ~/.ssh/id_rsa ec2-user@<instance-ip>

# Generate data with custom S3 location
./generate_tpch_data.sh 100 1000 your-bucket-name "your/custom/folder"

# This will create:
# s3://your-bucket-name/your/custom/folder/tpch_sf100/
# s3://your-bucket-name/your/custom/folder/tpch_sf1000/
```

### 5. Monitor Progress

```bash
# SSH to the instance and monitor
ssh -i ~/.ssh/id_rsa ec2-user@<instance-ip>
tail -f /var/log/cloud-init-output.log  # Instance setup logs
htop  # Monitor CPU/memory usage
df -h  # Monitor disk usage
```

### 6. Clean Up

```bash
terraform destroy
```

## Cost Optimization

### Estimated Costs (us-east-2)

| Scale Factor | Instance Type | Runtime | Instance Cost | Storage Cost | Total |
|--------------|---------------|---------|---------------|--------------|-------|
| SF=100       | c7i.4xlarge   | 1-2 hours | ~$1.75       | ~$0.30       | ~$2   |

### Cost Reduction Tips

1. **Use Spot Instances**: Add `spot_price` to reduce costs by 50-90%
2. **Regional Selection**: Choose cheaper regions if data transfer isn't critical
3. **Immediate Cleanup**: Run `terraform destroy` immediately after data generation
4. **Batch Generation**: Generate multiple scale factors in one session

## S3 Configuration

The infrastructure is pre-configured to use your existing `embucket-testdata` bucket with the folder structure you specified:

```hcl
existing_s3_bucket = "embucket-testdata"
s3_prefix = "tpch_data"
```

This will create the data at:
```
s3://embucket-testdata/tpch_data/sf_100/
```

## S3 Data Organization

Generated data will be organized in your S3 bucket as:
```
s3://embucket-testdata/tpch_data/
└── sf_100/
    ├── customer.parquet
    ├── lineitem.parquet
    ├── nation.parquet
    ├── orders.parquet
    ├── part.parquet
    ├── partsupp.parquet
    ├── region.parquet
    └── supplier.parquet
```

## Troubleshooting

### Common Issues

1. **Insufficient Disk Space**
   - Increase `root_volume_size` in terraform.tfvars
   - Monitor with `df -h` during generation

2. **Out of Memory**
   - Use larger instance type (more RAM)
   - Generate scale factors sequentially, not in parallel

3. **S3 Upload Failures**
   - Check IAM permissions
   - Verify S3 bucket exists and is accessible
   - Check AWS credentials with `aws sts get-caller-identity`

4. **Long Generation Times**
   - Use higher IOPS and throughput settings
   - Consider larger instance types
   - Monitor CPU usage with `htop`

### Monitoring Commands

```bash
# Check generation progress
tail -f ~/tpch-data/sf*/generate_sf*.log

# Monitor system resources
htop
iostat -x 1
df -h

# Check S3 upload progress
aws s3 ls s3://your-bucket/ --recursive --human-readable
```

## Advanced Configuration

### Custom Scale Factors

Edit `terraform.tfvars`:
```hcl
scale_factors = [50, 100, 500, 1000]
```

### Spot Instances

Add to `main.tf`:
```hcl
resource "aws_instance" "tpch_datagen" {
  # ... existing configuration ...
  
  instance_market_options {
    market_type = "spot"
    spot_options {
      max_price = "0.50"  # Adjust based on instance type
    }
  }
}
```

### Multiple Regions

Deploy in multiple regions for distributed data generation:
```bash
# Deploy in different regions
terraform apply -var="aws_region=us-west-2"
terraform apply -var="aws_region=eu-west-1"
```
