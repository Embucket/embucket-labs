# Environment Setup Guide

Quick setup guide for Embucket UI Lambda POC.

## Setup Steps

### 1. Create Lambda Function

1. AWS Console → Lambda → Create function
2. Runtime: **Node.js 20.x** (not Node.js 24)
3. Architecture: x86_64 or arm64
4. Create function

### 2. Build and Package

```bash
npm install
cd ui && npm install && cd ..
npm run build
npm run package
```

This creates `function.zip` (should be > 1MB).

### 3. Upload Code

Lambda Console → Code → Upload from → .zip file → Select `function.zip`

**⚠️ IMPORTANT:** After uploading, set the handler:
- Lambda → Configuration → Runtime settings → Edit
- **Handler:** `src.index.handler` (NOT `dist/src.index.handler`)

### 4. Create Function URL

Lambda → Configuration → Function URL → Create
- Auth type: **NONE**
- CORS: **Enabled**
- Copy the Function URL

### 5. Create RDS Database

1. AWS Console → RDS → Create database
2. Engine: PostgreSQL
3. Master username: `embucket`
4. Master password: (save this)
5. Public access: Yes (for POC)
6. Create database

### 6. Configure Security Group

RDS → Databases → Your DB → Connectivity & security → Security group → Edit inbound rules
- Add: Type: PostgreSQL, Port: 5432, Source: `0.0.0.0/0`

### 7. Create Schema

RDS → Query Editor → Connect to database → Run `schema.sql`

### 8. Set Environment Variables

Lambda → Configuration → Environment variables → Edit

| Variable | Value | Required |
|----------|-------|----------|
| `DB_HOST` | RDS endpoint (from Connectivity & security) | Yes |
| `DB_PORT` | `5432` | No |
| `DB_NAME` | `embucket-poc-db` | No |
| `DB_USER` | `embucket` | No |
| `DB_PASSWORD` | Your RDS password | Yes |
| `DB_SSL` | `true` | No |

## Troubleshooting 502 Errors

If you're getting a 502 Bad Gateway error, **start here:**

1. **Check CloudWatch Logs** (Most Important!)
   - Lambda → Monitor → View CloudWatch logs → Latest log stream
   - Look for error messages - they'll tell you exactly what's wrong

2. **Verify Handler Path**
   - Lambda → Configuration → Runtime settings → Edit
   - Handler should be: `src.index.handler` (NOT `dist/src.index.handler`)

3. **Check Environment Variables**
   - Lambda → Configuration → Environment variables
   - Must have: `DB_HOST` and `DB_PASSWORD`

Then check the detailed sections below:

### 1. Verify Handler Configuration

Lambda → Configuration → Runtime settings → Edit
- **Handler:** `src.index.handler` ⚠️ **This is the correct path!**
- **Runtime:** Node.js 20.x

**Why:** 
- The packaging script copies bundled code from `dist/bundle` → `src` in the zip
- Output is `src/index.js` (ES module format, since `package.json` has `"type": "module"`)
- Lambda handler format: `filename.exportName` → `index.handler`

**To verify:**
1. Unzip `function.zip` locally
2. You should see `src/index.js` (NOT `dist/src/index.js`)
3. Handler path: `src.index.handler`

### 2. Check CloudWatch Logs

Lambda → Monitor → View CloudWatch logs → Click on latest log stream

Look for:
- **"Cannot find module"** → Handler path is wrong
- **"Database connection timeout"** → RDS security group or environment variables
- **"DB_HOST is not defined"** → Missing environment variables
- **"Unexpected token"** → Build/packaging issue

### 3. Verify Environment Variables

Lambda → Configuration → Environment variables

**Required:**
- `DB_HOST` - Must be the full RDS endpoint (e.g., `my-db.xxxxx.us-east-1.rds.amazonaws.com`)
- `DB_PASSWORD` - Must match your RDS password

**Check:**
- No typos in variable names
- No extra spaces in values
- `DB_HOST` includes full endpoint (not just hostname)

### 4. Test Database Connection

From Lambda → Test tab, create a test event:
```json
{
  "httpMethod": "GET",
  "path": "/api/data"
}
```

Check logs for database connection errors.

### 5. Verify RDS Security Group

RDS → Your DB → Connectivity & security → Security group

**Inbound rules must allow:**
- Type: PostgreSQL
- Port: 5432
- Source: Lambda's security group OR `0.0.0.0/0` (for POC)

**To find Lambda's security group:**
- Lambda → Configuration → VPC (if configured)
- Or use `0.0.0.0/0` for POC testing

### 6. Check Function Timeout

Lambda → Configuration → General configuration → Edit
- **Timeout:** At least 10 seconds (default is 3s, may be too short for DB connection)

### 7. Verify Package Structure

The zip should contain:
```
function.zip
├── src/              ← Handler is here!
│   └── index.js      ← Bundled and minified (ES module format)
├── ui/
│   └── dist/
│       └── index.html
├── node_modules/
└── package.json
```

**Important:** 
- The packaging script copies bundled code from `dist/bundle` → `src`
- Output is `src/index.js` (ES module format, since `package.json` has `"type": "module"`)
- Handler path: `src.index.handler`

### 8. Common Fixes

**If handler path is wrong:**
- ✅ **Correct:** `src.index.handler` (file: `src/index.js`, export: `handler`)
- ❌ **Wrong:** `dist/src.index.handler` (this is the source path, not the packaged path)
- ❌ **Wrong:** `index.handler` (file is in `src/` folder)
- ❌ **Wrong:** `src.index` (must specify export name: `handler`)

**If database connection fails:**
- Verify RDS is publicly accessible
- Check security group allows Lambda's IP/VPC
- Verify `DB_HOST` is correct (no `https://` prefix)
- Try `DB_SSL=false` temporarily to test

**If module not found:**
- Rebuild: `npm run build && npm run package`
- Ensure `package.json` has `"type": "module"`
- Check that `dist/src/index.js` exists in zip

### 9. Quick Test

Create a minimal test handler to verify Lambda works:

```javascript
export const handler = async (event) => {
  return {
    statusCode: 200,
    body: JSON.stringify({ message: 'Lambda is working', env: process.env.DB_HOST ? 'DB configured' : 'DB missing' })
  };
};
```

If this works, the issue is in your Express app or database connection.