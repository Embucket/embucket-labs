---
title: Working with Apache Spark
description: Loading NYC Taxi dataset with Apache Spark
---

In this guide we will learn how to upload any data (not just NYC Taxi dataset) to Embucket using Apache Spark.
It is possible due to Embucket uses Apache Iceberg format for data storage and provides an Iceberg Catalog REST API to read and write data for external tools.

## Prerequisites

You will need `docker` installed on your machine to run this guide. You will also need to have `docker-compose` installed.
First, create this docker compose file:

```yaml
services:
  spark-iceberg:
    image: tabulario/spark-iceberg
    container_name: spark-iceberg
    build: spark/
    networks:
      iceberg_net:
    depends_on:
      - embucket
      - minio
    volumes:
      - ./warehouse:/home/iceberg/warehouse
      - ./notebooks:/home/iceberg/notebooks/notebooks
    environment:
      - AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
      - AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      - AWS_REGION=us-east-2
      - SPARK_DRIVER_MEMORY=16g
      - SPARK_EXECUTOR_MEMORY=16g
    ports:
      - 8888:8888
      # - 8080:8080
      # - 10000:10000
      # - 10001:10001
    entrypoint: /bin/sh
    command: >
      -c "
      echo \"
        spark.sql.extensions                   org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\n
        spark.sql.catalog.demo                 org.apache.iceberg.spark.SparkCatalog \\n
        spark.sql.catalog.demo.catalog-impl    org.apache.iceberg.rest.RESTCatalog \\n
        spark.sql.catalog.demo.uri             http://embucket:3000/catalog \\n
        spark.sql.catalog.demo.io-impl         org.apache.iceberg.aws.s3.S3FileIO \\n
        spark.sql.catalog.demo.warehouse       demo \\n
        spark.sql.catalog.demo.cache-enabled   false \\n
        spark.sql.catalog.demo.rest.access-key-id  AKIAIOSFODNN7EXAMPLE \\n
        spark.sql.catalog.demo.rest.secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \\n
        spark.sql.catalog.demo.rest.signing-region us-east-2 \\n
        spark.sql.catalog.demo.rest.sigv4-enabled  true \\n
        spark.sql.catalog.demo.s3.endpoint     http://warehouse.minio:9000 \\n
        spark.sql.defaultCatalog               demo \\n
        spark.eventLog.enabled                 true \\n 
        spark.eventLog.dir                     /home/iceberg/spark-events \\n
        spark.history.fs.logDirectory          /home/iceberg/spark-events \\n
        spark.sql.catalog.demo.s3.path-style-access  true \\n
      \" > /opt/spark/conf/spark-defaults.conf && ./entrypoint.sh notebook
      "
  embucket:
    image: embucket/embucket
    container_name: embucket
    depends_on:
      - mc
    networks:
      iceberg_net:
    ports:
      - 3000:3000
      - 8080:8080
    environment:
      - OBJECT_STORE_BACKEND=s3
      - SLATEDB_PREFIX=data/
      - AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
      - AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      - AWS_REGION=us-east-2
      - S3_BUCKET=mybucket
      - S3_ENDPOINT=http://warehouse.minio:9000
      - S3_ALLOW_HTTP=true
      - CATALOG_URL=http://embucket:3000/catalog
    volumes:
      - ./tmp:/tmp
  minio:
    image: minio/minio
    container_name: minio
    environment:
      - MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE
      - MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    volumes:
      - ./warehouse:/warehouse
    networks:
      iceberg_net:
        aliases:
          - warehouse.minio
    ports:
      - 9001:9001
      - 9000:9000
    command: ['server', '/warehouse', '--console-address', ':9001']
  mc:
    depends_on:
      - minio
    image: minio/mc
    container_name: mc
    networks:
      iceberg_net:
    environment:
      - AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
      - AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      - AWS_REGION=us-east-2
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc alias set minio http://warehouse.minio:9000 AKIAIOSFODNN7EXAMPLE wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/mybucket;
      /usr/bin/mc mb minio/mybucket;
      /usr/bin/mc anonymous set public minio/mybucket;
      tail -f /dev/null
      "
    healthcheck:
      test: ['CMD', '/usr/bin/mc', 'ls', 'minio/mybucket']
      interval: 10s
      timeout: 5s
      retries: 3
networks:
  iceberg_net:
```

## Step 1: Start the containers

Now, run `docker-compose up -d` to start the containers.
This spins 3 different containers:

- `minio` - this is S3 compatible MinIO server that we will use to store data
- `embucket` - this is Embucket server that we will use to store data
- `spark-iceberg` - this is running Apache Spark server with Iceberg support that we will use to load data

After the containers are started, you can access the Embucket UI at `http://localhost:8080` and Jupyter notebook at `http://localhost:8888`. Embucket is configured to store its metadata in a S3 bucket `mybucket`.

## Step 2: Create S3 based volume

In this guide we will first create a S3 based volume that we will use to store data (metadata is stored on S3 as well and this is configured with environment variables). This can be done either in the UI or using the internal API.

An example with `httpie` utility:

```bash
$ http http://localhost:3000/v1/metastore/volumes ident=demo type=s3 credentials:='{"credential_type":"access_key","aws-access-key-id":"AKIAIOSFODNN7EXAMPLE","aws-secret-access-key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}' bucket=mybucket endpoint='http://warehouse.minio:9000'
```

Next, we will create a database. This can be done either in the UI or using the internal API.

An example with `httpie` utility:

```bash
$ http http://localhost:3000/v1/metastore/databases ident=demo volume=demo
```

## Step 3: Load NYC Taxi dataset

Now, we will use Apache Spark to load NYC Taxi dataset into Embucket. We will use the `spark-iceberg` container that we started in Step 1. Open Jupyter notebook at `http://localhost:8888`, either open any notebook or create a new one and run the following code:

```
%%sql

CREATE DATABASE nyc IF NOT EXISTS;
```

NYC Taxi dataset is available at https://www.nyc.gov/site/tlc/about/tlc-data.page and is already downloaded in the spark-iceberg container:

```
!ls -l /home/iceberg/data/
```

Let's now load the data into Embucket:

```
df = spark.read.parquet("/home/iceberg/data/yellow_tripdata_2021-04.parquet")
df.write.saveAsTable("nyc.taxis")
```

Now, let's query the data:

```
%%sql

SELECT COUNT(*) as cnt
FROM nyc.taxis
```
