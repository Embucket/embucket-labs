# Embucket: A Snowflake-Compatible Lakehouse Platform  

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Embucket is an **Apache-Licensed**, **Snowflake-compatible** lakehouse platform designed with **openness** and **standardization** in mind. It provides a **Snowflake-compatible API**, supports **Iceberg REST catalogs**, and runs with **zero-disk architecture**—all in a lightweight, easy-to-deploy package.  

## 🚀 Quickstart  

Get started with Embucket in minutes using our pre-built **Docker image** available on [Quay.io](https://quay.io/repository/embucket/embucket).  

```sh
docker pull quay.io/embucket/embucket:latest
docker run -p 8888:8888 -p 3000:3000 quay.io/embucket/embucket:latest
```

Once the container is running, open:  

- **[localhost:8888](http://localhost:8888)** → UI Dashboard  
- **[localhost:3000/catalog](http://localhost:3000/catalog)** → Iceberg REST Catalog API  

## ✨ Features  

- ✅ **Snowflake-compatible** API & SQL syntax  
- ⚡ **Iceberg REST Catalog API**  
- 🛠️ **Zero-disk** architecture—no separate storage layer required  
- 🔄 **Upcoming**: Table maintenance  

## 📽️ Demo: Running dbt with Embucket  

This demo showcases how to use Embucket with **dbt** and execute the `snowplow_web` dbt project, treating Embucket as a Snowflake-compatible database.

### 🛠 Install Embucket  

```sh
# Clone and build the Embucket binary
git clone git@github.com:Embucket/control-plane-v2.git
cd control-plane-v2/
cargo build
```

### ⚙️ Configure and Run Embucket  

You can configure Embucket via **CLI arguments** or **environment variables**:

```sh
# Create a .env configuration file
cat << EOF > .env
# SlateDB storage settings
OBJECT_STORE_BACKEND=file
FILE_STORAGE_PATH=data
SLATEDB_PREFIX=sdb

# Optional: AWS S3 storage (leave blank if using local storage)
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
S3_BUCKET=
S3_ALLOW_HTTP=

# Iceberg Catalog settings
USE_FILE_SYSTEM_INSTEAD_OF_CLOUD=false
CONTROL_PLANE_URL=http://127.0.0.1
EOF

# Load environment variables (optional)
export $(grep -v '^#' .env | xargs)

# Start Embucket
./target/debug/nexus
```

### 🎨 (Optional) Configure and Run the UI  

To enable the web-based UI, run:  

```sh
# (UI setup instructions go here)
```

### 📦 Prepare Snowplow Source Data  

```sh
# Clone the dbt project with Snowplow package installed
git clone git@github.com:Embucket/compatibility-test-suite.git
cd compatibility-test-suite/dbt-snowplow/

# Set up a virtual environment and install dependencies
virtualenv .venv
source .venv/bin/activate
pip install dbt-core dbt-snowflake

# Start MinIO for local object storage
docker run -d --rm --name minio -p 9001:9001 -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server --console-address :9001 /data

# Create an S3-compatible bucket on MinIO
aws --endpoint-url http://localhost:9000 s3api create-bucket --bucket bucket

# Register Embucket storage profile & warehouse
http POST http://localhost:3000/v1/storage-profile type=aws region=us-east-2 bucket=bucket \
  credentials:='{"credential_type":"access_key","aws_access_key_id":"minioadmin","aws_secret_access_key":"minioadmin"}' \
  endpoint='http://localhost:9000'

http POST http://localhost:3000/v1/warehouse storage_profile_id=<storage-profile-id> prefix= name=snowplow

# Create Iceberg namespace  
http POST http://localhost:3000/catalog/v1/<warehouse-id>/namespaces namespace:='["public"]'
```

### 🔄 Run dbt Workflow  

```sh
cd compatibility-test-suite/dbt-snowplow/

# Activate virtual environment
source .venv/bin/activate

# Set Snowflake-like environment variables
export SNOWFLAKE_USER=user
export SNOWFLAKE_PASSWORD=xxx
export SNOWFLAKE_DB=snowplow

# Install the dbt Snowplow package
dbt deps

# Upload initial data
dbt seed

# Upload source data
# (Insert commands for loading source data here)

# Run dbt transformations
dbt run
```

---

## 🤝 Contributing  

We welcome contributions! To get involved:  

1. **Fork** the repository on GitHub  
2. **Create** a new branch for your feature or bug fix  
3. **Submit** a pull request with a detailed description  

For more details, see [CONTRIBUTING.md](CONTRIBUTING.md).  

## 📜 License  

This project is licensed under the **Apache 2.0 License**. See [LICENSE](LICENSE) for details.  

---

### 🔗 Useful Links  

- 📖 [Official Documentation](https://github.com/Embucket/docs)  
- 🐛 [Report Issues](https://github.com/Embucket/embucket/issues)  
- 💬 [Join the Community](https://discord.gg/your-community-link)  
