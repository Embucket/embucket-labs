---
title: Configuration
description: Configure Embucket.
---

Embucket can be configured using environment variables or a configuration file (`.env`). Configuration file is loaded from the current directory and consist of key-value pairs. Environment variables take precedence over configuration file.

```bash
cat .env.example
# Shared settings (if no env vars are used, it will default to the first option (memory) with the shared settings)
# Iceberg Catalog settings
# Set to your catalog url
CATALOG_URL=http://127.0.0.1:3000
# Optional: CORS settings
CORS_ENABLED=true
CORS_ALLOW_ORIGIN=http://127.0.0.1:8080

# Option 1 (Memory)
# SlateDB storage settings
OBJECT_STORE_BACKEND=memory

# Option 2 (File)
# SlateDB storage settings
#OBJECT_STORE_BACKEND=file
#FILE_STORAGE_PATH=storage
#SLATEDB_PREFIX=state

# Option 3 (S3)
# SlateDB storage settings
#OBJECT_STORE_BACKEND=s3
# Optional: AWS S3 storage (leave blank if using local storage)
#AWS_ACCESS_KEY_ID="<your_aws_access_key_id>"
#AWS_SECRET_ACCESS_KEY="<your_aws_secret_access_key>"
#AWS_REGION="<your_aws_region>"
#S3_BUCKET="<your_s3_bucket>"
#S3_ALLOW_HTTP=
```

```bash
$ embucketd --help
Usage: embucketd [OPTIONS]

Options:
  -b, --backend <BACKEND>
          Backend to use for state storage [env: OBJECT_STORE_BACKEND=file] [default: memory] [possible values: s3, file, memory]
  -s, --slatedb-prefix <SLATEDB_PREFIX>
          [env: SLATEDB_PREFIX=data/] [default: state]
      --host <HOST>
          Host to bind to [env: BUCKET_HOST=] [default: localhost]
      --port <PORT>
          Port to bind to [env: BUCKET_PORT=] [default: 3000]
      --assets-port <ASSETS_PORT>
          Port of web assets server to bind to [env: WEB_ASSETS_PORT=] [default: 8080]
      --catalog-url <CATALOG_URL>
          Iceberg catalog url [env: CATALOG_URL=] [default: http://127.0.0.1:3000]
      --cors-enabled <CORS_ENABLED>
          Enable CORS [env: CORS_ENABLED=true] [default: true] [possible values: true, false]
      --cors-allow-origin <CORS_ALLOW_ORIGIN>
          CORS Allow Origin [env: CORS_ALLOW_ORIGIN=http://localhost:8080] [default: http://localhost:8080]
  -d, --data-format <DATA_FORMAT>
          Data serialization format in Snowflake v1 API [env: DATA_FORMAT=json] [default: json]
      --jwt-secret <JWT_SECRET>
          JWT secret for auth [env: JWT_SECRET]
      --auth-demo-user <AUTH_DEMO_USER>
          User for auth demo [env: AUTH_DEMO_USER=] [default: embucket]
      --auth-demo-password <AUTH_DEMO_PASSWORD>
          Password for auth demo [env: AUTH_DEMO_PASSWORD=] [default: embucket]
      --tracing-level <TRACING_LEVEL>
          Tracing level, it can be overrided by *RUST_LOG* env var [env: TRACING_LEVEL=] [default: info] [possible values: off, info, debug, trace]
      --tracing-span-processor <TRACING_SPAN_PROCESSOR>
          Tracing span processor [env: span_processor=] [default: batch-span-processor] [possible values: batch-span-processor, batch-span-processor-experimental-async-runtime]
  -h, --help
          Print help
  -V, --version
          Print version

S3 Backend Options:
      --access-key-id <ACCESS_KEY_ID>
          AWS Access Key ID [env: AWS_ACCESS_KEY_ID=]
      --secret-access-key <SECRET_ACCESS_KEY>
          AWS Secret Access Key [env: AWS_SECRET_ACCESS_KEY=]
      --region <REGION>
          AWS Region [env: AWS_REGION=]
      --bucket <BUCKET>
          S3 Bucket Name [env: S3_BUCKET=]
      --endpoint <ENDPOINT>
          S3 Endpoint (Optional) [env: S3_ENDPOINT=]
      --allow-http <ALLOW_HTTP>
          Allow HTTP for S3 (Optional) [env: S3_ALLOW_HTTP=] [possible values: true, false]

File Backend Options:
      --file-storage-path <FILE_STORAGE_PATH>
          Path to the directory where files will be stored [env: FILE_STORAGE_PATH=data/]
```

## Generic options

`--no-bootstrap` option is used to disable bootstrap process. Bootstrap routine creates in-memory volume, default database (named `embucket`) and default schema (named `public`).

`DATA_FORMAT` option is used to set data format in Snowflake v1 API. It can be `json` or `arrow`.

## HTTP configuration

Embucket uses HTTP to serve the UI and API. This is configured using `HOST` and `PORT` environment variables. Once launched, API is available at `http://HOST:PORT`.

## Metadata configuration

Embucket uses SlateDB to persist metadata. This is configured using `SLATEDB_PREFIX` and `OBJECT_STORE_BACKEND` environment variables.
There are three options for SlateDB storage:

1. Memory
2. File system
3. Object storage

Memory and file system backends are for testing and development purposes. Object storage is recommended for production use.

When using file as a backend, `FILE_STORAGE_PATH` environment variable should be set to the directory where files will be stored. `SLATEDB_PREFIX` environment variable should be set to the prefix of the SlateDB database.

When using object storage, `OBJECT_STORE_BACKEND` environment variable should be set to `s3`. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`, `S3_ENDPOINT`, `S3_ALLOW_HTTP` environment variables should be set to the values of the object storage provider.

## UI configuration

Embucket uses `HOST` and `ASSETS_PORT` environment variables to serve the UI and API. Once launched, you can access the UI at `http://HOST:ASSETS_PORT`.
`AUTH_DEMO_USER` and `AUTH_DEMO_PASSWORD` environment variables are used to authenticate the demo user (credentials to login in the UI).

User is required to set `JWT_SECRET` environment variable to enable authentication (this should be set to some random string).

`CORS_ENABLED` and `CORS_ALLOW_ORIGIN` environment variables are used to configure CORS. `CORS_ENABLED` should be set to `true` to enable CORS. `CORS_ALLOW_ORIGIN` should be set to the origin of the UI (e.g. `http://localhost:8080` or `https://acme.com`).

## Iceberg catalog configuration

Embucket provides an Iceberg catalog REST API at `http://HOST:PORT/catalog`. `CATALOG_URL` environment variable should be set to the URL of the Iceberg catalog, this is required for external clients to be able to connect to the catalog when requesting connection options via `v1/config` endpoint.
