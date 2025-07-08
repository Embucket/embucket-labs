---
title: Architecture
description: High-level overview of Embucket's lakehouse architecture and core components
---

Embucket is the “Snowflake bring-your-own-cloud” option built on the principle of **radical simplicity**: a statically-linked Rust executable that speaks Snowflake’s SQL & REST while storing every byte—data _and_ metadata—directly in your object store.

This page provides a high-level overview of Embucket's architecture, designed for users who want to understand how the system works without diving into source code.

## System Overview

At its core, Embucket follows a **zero-disk architecture** where all persistent state—both data and metadata—resides in object storage like Amazon S3. This design eliminates traditional database dependencies and operational complexity while providing a fully distributed, horizontally scalable lakehouse platform.

<!--![System Overview](assets/system-overview.png)-->

## Core Concepts

Embucket is a thoughtful integration of best-in-class open-source projects, which provides a reliable foundation.

### Radical Simplicity

- Single executable—embucket bundles API, planner, query engine, catalog and UI.
- Zero-disk architecture—no local volumes; durability and sharing come from the object store.
- Stateless nodes—all persistent state lives in S3, so any node can be replaced instantly.

### Open-Source Foundation

Embucket embraces and contributes to three battle-tested projects:

- For execution: Apache DataFusion (Rust, vectorised, ANSI SQL, and pluggable)
- For storage: Apache Iceberg (ACID tables, schema evolution, time-travel—without leaving S3)
- For metadata: SlateDB (Embedded LSM that writes directly to object storage—no RDS or etcd required)

### Snowflake Compatibility

Embucket parses Snowflake-flavoured SQL (ALTER SESSION, semi-structured types, etc.) and implements the v1 Snowflake REST API allowing tools like `dbt`, `snowflake-cli`, and Apache Superset to treat it as a Snowflake endpoint out of the box.

## The Lakehouse Architecture

Embucket’s lakehouse philosophy is to treat the object store as the database: all persistent state—both your data (in Parquet files) and the Iceberg metadata that defines it—resides in your object store.

- Tables are Iceberg directories inside your bucket (s3://data/warehouse/db/table/).
- Metadata—catalog changes, role grants, query history—is persisted by SlateDB as immutable SSTs in the same bucket.
- Queries are compiled and executed in-memory by DataFusion, reading Iceberg data lazily and writing new snapshots atomically.

### Iceberg integration

Embucket uses Iceberg format internally for its data storage and provides an Iceberg Catalog REST API. This allows other clients (like Apache Spark and Trino) to query your data using the same Iceberg metadata.

### Metadata persistence

To achieve a true single-dependency architecture, Embucket needed a way to manage its internal metadata without forcing users to run a separate database like PostgreSQL or MySQL. The solution is SlateDB.

SlateDB is a simple, embedded key-value database used by Embucket to persist metadata directly to object storage. It is purpose-built to store the pointers to your Iceberg metadata files, ensuring that the entire state of your lakehouse remains within your designated S3 bucket. This design choice is a cornerstone of our "Radical Simplicity" principle.

### Query execution

Embucket uses Apache DataFusion as its query engine. DataFusion is a powerful and extensible in-memory query engine written in Rust, leveraging the Apache Arrow ecosystem for columnar data processing.

We chose DataFusion for several reasons:

- Performance: Its columnar, vectorized processing engine provides excellent query performance.
- Extensibility: It allows us to customize and extend the SQL dialect and engine functionality.
- Ecosystem: As part of the Apache Arrow project, it benefits from a vibrant community and a robust development pace.
