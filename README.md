# About 

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


This is Embucket: an Apache-Licensed Snowflake compatible lakehouse platform built on principles of opennes and standardization. 

# Quickstart

A docker image is available on [quay.io](https://quay.io/repository/embucket/embucket). This preconfigured image demonstrates how to use Embucket.

```sh
docker pull quay.io/embucket/embucket:latest
docker run embucket/embucket:latest
```

After starting the container, open your browser and navigate to [localhost:8888](localhost:8888) to view the UI. 
The Iceberg REST catalog API is accessible at [localhost:3000/catalog](localhost:3000/catalog).

# Features

 * Snowflake on-the-wire compatible API
 * Snowflake SQL syntax compatible query engine
 * Zero-disk architecture: bucket is all you need
 * Iceberg REST catalog API
 * (upcoming) Table maintenance

# Demo

In this demo, we show how to use Embucket to execute the `snowplow_web` dbt project, with Snowflake serving as the target database.

### Install Embucket

```sh
# Download and build embucket binary
git checkout git@github.com:Embucket/control-plane-v2.git
cd control-plane-v2/
cargo build
```

### Configure and run Embucket

One can configure embucket binary specifing arguments on the cli or passing as environment variables:

```sh
cat << EOF > .env
# Slatedb
OBJECT_STORE_BACKEND=file
FILE_STORAGE_PATH=data
SLATEDB_PREFIX=sdb
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
S3_BUCKET=
S3_ALLOW_HTTP=

# Iceberg Catalog
USE_FILE_SYSTEM_INSTEAD_OF_CLOUD=false
CONTROL_PLANE_URL=http://127.0.0.1
EOF

# Optional - embucket binary reads `.env` automatically
export $(grep -v '^#' .env | xargs)

# Run the embucket
./target/debug/nexus
```

### (Optional) Configure and run UI

```sh
```

### Prepare source data for snowplow

```sh
# Download dbt project with snowplow package installed
git checkout git@github.com:Embucket/compatibility-test-suite.git
cd compatibility-test-suite/dbt-snowplow/

# Install dbt and snowflake connector
virtualenv .venv
. .venv/bin/activate
pip install dbt-core dbt-snowflake


# Use local minio as object storage
docker run -d --rm --name minio -p 9001:9001 -p 9000:9000 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server --console-address :9001 /data
aws --endpoint-url http://localhost:9000 s3api create-bucket --bucket bucket

# Create embucket warehouse and cloud storage profile
http localhost:3000/v1/storage-profile type=aws region=us-east-2 bucket=bucket credentials:='{"credential_type":"access_key","aws_access_key_id":"minioadmin","aws_secret_access_key":"minioadmin"}' endpoint='http://localhost:9000'
http localhost:3000/v1/warehouse storage_profile_id=<storage-profile-id> prefix= name=snowplow

# Create Iceberg namespace 
http localhost:3000/catalog/v1/<warehouse-id>/namespaces namespace:='["public"]'


```

### Run dbt workflow

```sh
cd compatibility-test-suite/dbt-snowplow/

# Activate virtual env
. .venv/bin/activate

# Setup snowflake variables
export SNOWFLAKE_USER=user
export SNOWFLAKE_PASSWORD=xxx
export SNOWFLAKE_DB=snowplow

# Install snowplow_web dbt package
dbt deps
# Upload initial data
dbt seed
# Upload source data
...

# Run dbt workflow
dbt run
```
