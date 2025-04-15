# Embucket: Snowflake compatible lakehouse platform  

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Embucket is an **Apache-Licensed**, **Snowflake-compatible** lakehouse platform designed with **openness** and **standardization** in mind. It provides a **Snowflake-compatible API**, supports **Iceberg REST catalogs**, and runs with **zero-disk architecture**—all in a lightweight, easy-to-deploy package.  

## Quickstart  

Get started with Embucket in minutes using our pre-built **Docker image** available on [Quay.io](https://quay.io/repository/embucket/embucket).  

```sh
docker pull 767397688925.dkr.ecr.us-east-2.amazonaws.com/embucket/control-plane
docker run -p 3000:3000 embucket/control-plane
```

Once the container is running, open:  

- **[localhost:8888](http://localhost:8888)** → UI Dashboard  
- **[localhost:3000/catalog](http://localhost:3000/catalog)** → Iceberg REST Catalog API  

## Features  

- **Snowflake-compatible** API & SQL syntax  
- **Iceberg REST Catalog API**  
- **Zero-disk** architecture—no separate storage layer required  
- **Upcoming**: Table maintenance  

## Demo: Running dbt with Embucket  

This demo showcases how to use Embucket with **dbt** and execute the `snowplow_web` dbt project, treating Embucket as a Snowflake-compatible database.

Prerequisites:
* Install Rust (https://www.rust-lang.org/tools/install)
* Install Python (https://www.python.org/downloads/)
* Install our test dataset (REDACTED, seriously though github probaly won't allow big files)
* Install virtualenv (https://virtualenv.pypa.io/en/latest/)
* (Optional) Install NodeJS LTS (https://nodejs.org/en/download)
* (Optional) Install PNPM (https://pnpm.io/installation)

### Install Embucket  

```sh
# Clone and build the Embucket binary
git clone git@github.com:Embucket/embucket.git
cd embucket/
cargo build
```

### Configure and run Embucket  

You can configure Embucket via **CLI arguments** or **environment variables**:

```sh
# Create a .env configuration file
cat << EOF > .env
# SlateDB storage settings
OBJECT_STORE_BACKEND=memory
FILE_STORAGE_PATH=data
SLATEDB_PREFIX=sdb

# Optional: AWS S3 storage (leave blank if using local storage)
AWS_ACCESS_KEY_ID="1"
AWS_SECRET_ACCESS_KEY="2"
AWS_REGION=
S3_BUCKET=
S3_ALLOW_HTTP=

# Iceberg Catalog settings
CONTROL_PLANE_URL=http://127.0.0.1:3000

# Dialect
SQL_PARSER_DIALECT=snowflake
EOF

# Load environment variables (optional)
export $(grep -v '^#' .env | xargs)

# Start Embucket
./target/debug/bucketd
```

### (Optional) Configure and run the UI  

To enable the web-based UI, run:  

```sh
# (UI setup instructions go here)
cp .env.example .env

pnpm i

pnpm codegen

pnpm dev

open http://localhost:5173
```

**Note**: `bucketd` must be run with `--cors-enabled` and `--cors-allow-origin=http://localhost:5173` in order for the UI to be able to authenticate properly.

### Run dbt workflow  

```sh
# Clone the dbt project with Snowplow package installed
git clone git@github.com:Embucket/compatibility-test-suite.git
cd compatibility-test-suite/dbt-snowplow/

# Set up a virtual environment and install dependencies
virtualenv .venv
source .venv/bin/activate
pip install dbt-core dbt-snowflake

# Activate virtual environment
source .venv/bin/activate

# Set Snowflake-like environment variables
export SNOWFLAKE_USER=user
export SNOWFLAKE_PASSWORD=xxx
export SNOWFLAKE_DB=snowplow
export SNOWFLAKE_SCHEMA=public
export SNOWFLAKE_WAREHOUSE=snowplow

# Install the dbt Snowplow package
dbt deps

# Upload source data
python3 upload.py

# Upload initial data
dbt seed

# Run dbt transformations
dbt run
```

---

## Contributing  

We welcome contributions! To get involved:  

1. **Fork** the repository on GitHub  
2. **Create** a new branch for your feature or bug fix  
3. **Submit** a pull request with a detailed description  

For more details, see [CONTRIBUTING.md](CONTRIBUTING.md).  

## Contributors

<!-- readme: contributors -start -->
<!-- readme: contributors -end -->

## License  

This project is licensed under the **Apache 2.0 License**. See [LICENSE](LICENSE) for details.  

---

### Useful links  

- [Official Documentation](https://github.com/Embucket/embucket/docs)  
- [Report Issues](https://github.com/Embucket/embucket/issues)  

