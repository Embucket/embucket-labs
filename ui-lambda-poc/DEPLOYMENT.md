# AWS Lambda Deployment Guide

This guide will help you deploy your `index.js` Express application to AWS Lambda.

## Prerequisites

1. **AWS Account** with Lambda access
2. **Node.js 20.x** (or 18.x) runtime
3. **Dependencies installed**: Run `pnpm install` in the root directory
4. **UI built**: Run `pnpm run build:ui` to build the React app

## Step 1: Build and Package

```bash
# Install dependencies (if not already done)
pnpm install

# Build the UI
pnpm run build:ui

# Create deployment package
pnpm run package
```

This creates `function.zip` in the root directory.

## Step 2: Create Lambda Function

### Option A: AWS Console

1. Go to [AWS Lambda Console](https://console.aws.amazon.com/lambda/)
2. Click **"Create function"**
3. Choose **"Author from scratch"**
4. Configure:
   - **Function name**: `embucket-ui-lambda` (or your preferred name)
   - **Runtime**: `Node.js 20.x` (or `Node.js 18.x`)
   - **Architecture**: `x86_64`
   - **Permissions**: Create a new role with basic Lambda permissions (you can add RDS access later)
5. Click **"Create function"**
6. **IMPORTANT**: After creating, go to **"Code"** tab and set:
   - **Handler**: `index.handler` (NOT `src/index.handler` or `src/index.js`)

### Option B: AWS CLI

```bash
aws lambda create-function \
  --function-name embucket-ui-lambda \
  --runtime nodejs20.x \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role \
  --handler index.handler \
  --zip-file fileb://function.zip \
  --timeout 30 \
  --memory-size 512
```

**Note**: The handler must be `index.handler` (not `src/index.handler`). The `index.js` file is at the root of the zip package.

## Step 3: Upload Deployment Package

### Option A: AWS Console

1. In your Lambda function, scroll to **"Code source"**
2. Click **"Upload from"** → **".zip file"**
3. Select `function.zip`
4. Click **"Save"**

### Option B: AWS CLI

```bash
aws lambda update-function-code \
  --function-name embucket-ui-lambda \
  --zip-file fileb://function.zip
```

## Step 4: Configure Function Settings

### Basic Configuration

1. Go to **"Configuration"** → **"General configuration"**
2. Click **"Edit"**
3. Set:
   - **Timeout**: `30 seconds` (or more if needed)
   - **Memory**: `512 MB` (or more for better performance)
4. Click **"Save"**

### Environment Variables

1. Go to **"Configuration"** → **"Environment variables"**
2. Click **"Edit"**
3. Add the following variables:

| Variable | Value | Required |
|----------|-------|----------|
| `DB_HOST` | Your RDS endpoint (e.g., `mydb.xxxxx.us-east-1.rds.amazonaws.com`) | Yes (if using DB) |
| `DB_PORT` | `5432` | No |
| `DB_NAME` | `embucket-poc-db` | No |
| `DB_USER` | Your database username | Yes (if using DB) |
| `DB_PASSWORD` | Your database password | Yes (if using DB) |
| `DB_SSL` | `true` (for RDS) | No |

4. Click **"Save"**

## Step 5: Create Function URL

1. Go to **"Configuration"** → **"Function URL"**
2. Click **"Create function URL"**
3. Configure:
   - **Auth type**: `NONE` (or `AWS_IAM` for security)
   - **CORS**: Enable if needed (your code already handles CORS)
4. Click **"Save"**
5. Copy the **Function URL** - this is your API endpoint!

## Step 6: Configure VPC (If Using RDS)

If your RDS database is in a VPC, Lambda needs VPC access:

1. Go to **"Configuration"** → **"VPC"**
2. Click **"Edit"**
3. Select:
   - **VPC**: Your RDS VPC
   - **Subnets**: At least 2 subnets in different AZs
   - **Security groups**: A security group that allows outbound to RDS port 5432
4. Click **"Save"**

**Important**: VPC configuration adds cold start latency. Consider using RDS Proxy for better performance.

## Step 7: Test Your Function

### Test via Function URL

```bash
# Health check
curl https://YOUR_FUNCTION_URL.lambda-url.us-east-1.on.aws/api/health

# Get data
curl https://YOUR_FUNCTION_URL.lambda-url.us-east-1.on.aws/api/data

# Test UI
open https://YOUR_FUNCTION_URL.lambda-url.us-east-1.on.aws/
```

### Test via Lambda Console

1. Go to **"Test"** tab
2. Create a new test event (or use default)
3. Click **"Test"**
4. Check the execution result

## Troubleshooting

### Common Issues

1. **Timeout errors**
   - Increase Lambda timeout (up to 15 minutes)
   - Check VPC configuration if using RDS
   - Verify security groups allow outbound connections

2. **Database connection errors**
   - Verify environment variables are set correctly
   - Check RDS security group allows Lambda's security group
   - Ensure Lambda is in the same VPC as RDS (or use public endpoint with proper security)

3. **Package too large**
   - If `function.zip` > 50MB, consider:
     - Using Lambda Layers for `node_modules`
     - Removing unnecessary dependencies
     - Using `npm prune --production` before packaging

4. **Handler not found / ERR_REQUIRE_ESM error**
   - **CRITICAL**: Verify handler is set to: `index.handler` (NOT `src/index.handler`)
   - The handler path in Lambda must match: `index.handler` (file at root: `index.js`, exports `handler`)
   - If you see "require() of ES Module" error, it means Lambda is looking in the wrong path
   - To fix: Go to Lambda → Configuration → Runtime settings → Edit → Set Handler to `index.handler`
   - Ensure `index.js` is in the root of the zip (verify with `unzip -l function.zip | grep index.js`)

5. **CORS errors**
   - Your code already handles CORS, but verify Function URL CORS settings
   - Check browser console for specific CORS errors

### Viewing Logs

```bash
# View recent logs
aws logs tail /aws/lambda/embucket-ui-lambda --follow

# Or use CloudWatch Console
# Go to: CloudWatch → Log groups → /aws/lambda/embucket-ui-lambda
```

## Updating the Function

After making changes:

```bash
# Rebuild UI (if UI changed)
pnpm run build:ui

# Recreate package
pnpm run package

# Update Lambda
aws lambda update-function-code \
  --function-name embucket-ui-lambda \
  --zip-file fileb://function.zip
```

## Cost Optimization

- **Memory**: Start with 512MB, adjust based on performance
- **Timeout**: Set appropriate timeout (don't use max 15min unless needed)
- **Reserved Concurrency**: Set if you want to limit concurrent executions
- **Provisioned Concurrency**: Use for consistent low-latency (costs more)

## Security Best Practices

1. **Use AWS_IAM auth** for Function URL instead of NONE
2. **Store secrets in AWS Secrets Manager** instead of environment variables
3. **Use VPC endpoints** for RDS access (no internet gateway needed)
4. **Enable CloudWatch Logs encryption**
5. **Use least-privilege IAM roles**

## Next Steps

- Set up CI/CD pipeline for automated deployments
- Configure custom domain for Function URL
- Set up CloudWatch alarms for errors
- Implement API Gateway if you need more advanced features

