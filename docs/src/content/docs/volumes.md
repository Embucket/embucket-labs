---
title: Volumes
description: Configure volumes
---

Embucket is a Snowflake compatible data lakehouse, it uses Iceberg as internal storage layer and also provides an Iceberg REST API. In this guide we will explain how to use volumes to persist data and metadata.

## Iceberg warehouses

Iceberg introduced the concept of warehouses to define where data and metadata are stored. There are namespaces, tables and views. Each iceberg warehouse groups together collection of namespaces, tables and views. This is very different from Snowflake warehouse, which is a virtual cluster of compute resources.

Embucket inherits this concept and extends it to define where data and metadata are stored. Each volume basically is a pointer to object storage and credentials to access it. At the moment, volume is required to create a database.

To align with Snowflake, Embucket uses following mapping between iceberg concepts:

| Iceberg   | Embucket |
| --------- | -------- |
| Warehouse | Database |
| Namespace | Schema   |
| Table     | Table    |

Nested schemas aren't supported in Embucket, all schemas are flat.

## Creating a volume

To create a volume, you can use the UI or internal API.

Note: there are different types of volumes, but memory and filesystem volumes are only for testing purposes and aren't guaranteed to work with all different deployments. Only S3 volumes are guaranteed to work with all different deployments.

Here is an example of creating a volume using `httpie`, it uses locally run MinIO server (runs on port 9000):

```bash
http http://localhost:3000/v1/metastore/volumes ident=myvolume type=s3 credentials:='{"credential_type":"access_key","aws-access-key-id":"minioadmin","aws-secret-access-key":"minioadmin"}' bucket=embucket-lakehouse endpoint='http://localhost:9000'
```

Same can be done in the UI:

![Volume creation](/assets/volume-creation.png)

## Creating a database

To create a database, you can use the UI or internal API.

Here is an example of creating a database using `httpie`:

```bash
http http://localhost:3000/v1/metastore/databases ident=mydatabase volume=myvolume
```

Single volume can be used for multiple databases.
