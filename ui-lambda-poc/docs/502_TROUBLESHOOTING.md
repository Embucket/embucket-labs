# 502 Error Troubleshooting Guide

## Step 1: Check CloudWatch Logs (CRITICAL!)

**This is the most important step** - CloudWatch logs will tell you exactly what's wrong.

1. Go to AWS Lambda Console → Your Function → Monitor tab
2. Click "View CloudWatch logs"
3. Click on the latest log stream
4. Look for error messages

### Common Error Messages:

- **"Cannot find module 'express'"** → Missing dependencies in zip
- **"SyntaxError: Unexpected token"** → Build/packaging issue
- **"Error: Cannot find module"** → Handler path or module structure issue
- **"Task timed out"** → Handler taking too long to initialize
- **"DB_HOST is not defined"** → Missing environment variables (but this usually gives 500, not 502)

## Step 2: Test with Minimal Handler

To isolate whether the issue is with your code or Lambda configuration:

1. **Temporarily change handler to testHandler:**
   - Lambda → Configuration → Runtime settings → Edit
   - Change handler from `src.index.handler` to `src.index.testHandler`
   - Save

2. **Test the URL again:**
   ```bash
   curl https://wphkbawzth3wuuj3wdnv6tl36e0pqaoi.lambda-url.us-east-2.on.aws/
   ```

3. **If testHandler works:**
   - The issue is in your main handler code (likely initialization error)
   - Check CloudWatch logs for initialization errors
   - Verify dependencies are included

4. **If testHandler also fails:**
   - Handler path or module loading issue
   - Check CloudWatch logs for module loading errors

## Step 3: Verify Dependencies Are Included

The bundled code requires these dependencies in `node_modules`:
- `express`
- `serverless-http`
- `pg`

**To verify:**

1. Download the zip from Lambda (if possible) or rebuild locally
2. Unzip `function.zip`
3. Check that `node_modules/express`, `node_modules/serverless-http`, `node_modules/pg` exist

**If missing, rebuild:**
```bash
cd ui-lambda-poc
npm install
npm run build
npm run package
```

## Step 4: Check Function Timeout

502 errors can occur if the handler times out during initialization:

1. Lambda → Configuration → General configuration → Edit
2. Set **Timeout** to at least **30 seconds** (default is 3s)
3. Test again

## Step 5: Verify Environment Variables

While missing env vars usually cause 500 errors, they can cause initialization failures:

1. Lambda → Configuration → Environment variables
2. Verify:
   - `DB_HOST` is set (can be a dummy value for testing)
   - `DB_PASSWORD` is set (can be a dummy value for testing)

**Note:** The handler should work even without DB connection - it will just return errors for DB routes.

## Step 6: Check Architecture Compatibility

You're using **arm64** architecture. Verify dependencies support it:

1. The `pg` native module should work on arm64
2. If you see "Cannot find module" errors, try switching to **x86_64** temporarily

## Step 7: Verify Zip Structure

The zip should contain:
```
function.zip
├── src/
│   └── index.js          ← Handler file (ES module)
├── ui/
│   └── dist/
│       └── index.html
├── node_modules/
│   ├── express/
│   ├── serverless-http/
│   └── pg/
└── package.json          ← Must have "type": "module"
```

## Quick Fix: Rebuild and Re-upload

If all else fails, try a clean rebuild:

```bash
cd ui-lambda-poc

# Clean everything
rm -rf dist node_modules ui/node_modules ui/dist function.zip

# Rebuild
npm install
cd ui && npm install && cd ..
npm run build
npm run package

# Upload function.zip to Lambda
```

## Still Getting 502?

**Share the CloudWatch log output** - that will tell us exactly what's wrong!

Common issues:
1. Module loading error → Check dependencies
2. Initialization timeout → Increase timeout
3. Syntax error → Rebuild
4. Missing file → Check zip structure

