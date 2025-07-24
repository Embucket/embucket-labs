---
title: Iceberg catalog
description: Use Embucket as an Iceberg Catalog.
---

Embucket provides an Iceberg Catalog REST API that allows other clients (like Apache Spark and Trino) to query and write to your data.

This guide will show you how to connect to Embucket using `pyiceberg`, a python library for working with Iceberg tables.

## Step 1: Install pyiceberg

```bash
pip install 'pyiceberg[all]'
```

## Step 2: Ensure Embucket is running

Embucket must be running for this guide to work. If you haven't already, follow the [quick start](/essentials/quick-start) to get started.

## Step 3: Create a volume

> Note: If you are running Embucket in docker, you will need to use a volume that is accessible from the client machine. Memory volumes store data in embucket memory and Iceberg clients won't be able to read from them. Filesystem volume store data on the disk and Iceberg clients will be able to read from them given embucket and iceberg clients are running on the same machine (i.e. running embucket in docker also won't work). Only S3 volumes are guaranteed to work with all different deployments.

As explained in the [Volume](/essentials/volumes) guide, volumes are used to specify where data is persisted between runs of Embucket. This is where we will store our Iceberg tables. Volume can be create in the UI or using internal API. We will be creating filesystem based volume here, path needs to be absolute.

```python
import requests

response = requests.post(
    "http://127.0.0.1:3000/v1/metastore/volumes",
    json={
        "ident": "demo",
        "type": "file",
        "path": "/Users/ramp/tmp/demo",
    })
```

Next, let's create a database: in Embucket to create a database a volume is required. Database could also be created in the UI or using internal API. At the moment, `CREATE DATABASE` is not supported (since database requires a volume to be specified).

```python
response = requests.post(
    "http://127.0.0.1:3000/v1/metastore/databases",
    json={
        "ident": "demo",
        "volume": "demo",
    })
```

## Step 4: Connect to Embucket

```python
from pyiceberg.catalog import load_catalog
# Connect to Embucket
catalog = load_catalog(
    type="rest",
    uri="http://127.0.0.1:3000/catalog",
    warehouse="demo",
)
```

Here we specify catalog type to be used (`rest`), its URI and a warehouse. In Iceberg entities warehouse is used to group namespaces, and namespaces group tables and views. In Snowflake (and in Apache Datafusion that is used as a query engine), databases are used to group schemas, and schemas group tables and views. Thus, here we use warehouse name as a database name. `demo` in the example above is the name of the database:

```sh
$ snow sql -c local

> show databases;
+----------------------------------------------------------------+
| created_on | name     | kind     | database_name | schema_name |
|------------+----------+----------+---------------+-------------|
| None       | slatedb  | STANDARD | None          | None        |
| None       | demo     | STANDARD | None          | None        |
| None       | embucket | STANDARD | None          | None        |
+----------------------------------------------------------------+
```

Let's take the NYC Taxi dataset, and write this to a table.

First download one month of data:

```bash
curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet -o yellow_tripdata_2023-01.parquet
```

Load it into your PyArrow dataframe:

```python
import pyarrow.parquet as pq

df = pq.read_table("yellow_tripdata_2023-01.parquet")
```

Create a new Iceberg table:

```python
catalog.create_namespace("public")
table = catalog.create_table(
    "public.taxi_dataset",
    schema=df.schema,
)
```

Append the dataframe to the table:

```python
table.append(df)
len(table.scan().to_arrow())
```

Now we can query the table with snowflake-cli:

```bash
$ snow sql -c local

 > select * from demo.public.taxi_dataset limit 10;
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| VendorID | tpep_pickup_datetime | tpep_dropoff_datetime | passenger_count | trip_distance | RatecodeID | store_and_fwd_flag | PULocationID | DOLocationID | payment_type | fare_amount | extra | mta_tax | tip_amount | tolls_amount | improvement_surcharge | total_amount | congestion_surcharge | airport_fee |
|----------+----------------------+-----------------------+-----------------+---------------+------------+--------------------+--------------+--------------+--------------+-------------+-------+---------+------------+--------------+-----------------------+--------------+----------------------+-------------|
| 2        | 2023-01-22 21:37:32  | 2023-01-22 21:53:13   | 3.0             | 3.12          | 1.0        | N                  | 230          | 211          | 1            | 17.0        | 1.0   | 0.5     | 4.4        | 0.0          | 1.0                   | 26.4         | 2.5                  | 0.0         |
| 2        | 2023-01-22 21:56:16  | 2023-01-22 22:05:41   | 1.0             | 1.94          | 1.0        | N                  | 114          | 13           | 1            | 12.1        | 1.0   | 0.5     | 3.42       | 0.0          | 1.0                   | 20.52        | 2.5                  | 0.0         |
| 2        | 2023-01-22 21:39:05  | 2023-01-22 21:43:20   | 1.0             | 0.37          | 1.0        | N                  | 229          | 229          | 1            | 5.8         | 1.0   | 0.5     | 0.25       | 0.0          | 1.0                   | 11.05        | 2.5                  | 0.0         |
| 2        | 2023-01-22 21:50:14  | 2023-01-22 21:52:57   | 1.0             | 0.85          | 1.0        | N                  | 262          | 263          | 1            | 5.8         | 1.0   | 0.5     | 2.16       | 0.0          | 1.0                   | 12.96        | 2.5                  | 0.0         |
| 2        | 2023-01-22 21:57:31  | 2023-01-22 22:02:46   | 1.0             | 1.23          | 1.0        | N                  | 141          | 75           | 2            | 7.9         | 1.0   | 0.5     | 0.0        | 0.0          | 1.0                   | 12.9         | 2.5                  | 0.0         |
| 2        | 2023-01-22 21:22:44  | 2023-01-22 21:26:06   | 1.0             | 0.44          | 1.0        | N                  | 256          | 256          | 2            | 5.8         | 1.0   | 0.5     | 0.0        | 0.0          | 1.0                   | 8.3          | 0.0                  | 0.0         |
| 2        | 2023-01-22 21:38:28  | 2023-01-22 21:46:46   | 1.0             | 1.78          | 1.0        | N                  | 79           | 170          | 1            | 10.7        | 1.0   | 0.5     | 3.14       | 0.0          | 1.0                   | 18.84        | 2.5                  | 0.0         |
| 2        | 2023-01-22 21:49:46  | 2023-01-22 22:02:26   | 1.0             | 2.42          | 1.0        | N                  | 170          | 143          | 1            | 14.9        | 1.0   | 0.5     | 3.98       | 0.0          | 1.0                   | 23.88        | 2.5                  | 0.0         |
| 2        | 2023-01-22 21:59:58  | 2023-01-22 22:36:35   | 1.0             | 22.26         | 1.0        | N                  | 132          | 241          | 1            | 83.5        | 1.0   | 0.5     | 10.0       | 6.55         | 1.0                   | 103.8        | 0.0                  | 1.25        |
| 2        | 2023-01-22 21:15:44  | 2023-01-22 21:40:22   | 2.0             | 9.14          | 1.0        | N                  | 264          | 151          | 1            | 38.7        | 6.0   | 0.5     | 13.81      | 6.55         | 1.0                   | 70.31        | 2.5                  | 1.25        |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

 > select count(*) from demo.PUBLIC.taxi_dataset;
+----------+
| count(*) |
|----------|
| 3066766  |
+----------+
```

**I was running embucket in docker and thus path was not accessible from host machine.**
