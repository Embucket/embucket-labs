---
title: Introduction
description: Meet Embucket – an open-source, Snowflake-compatible OLAP lakehouse that marries the simplicity of a single binary with the openness of Apache Iceberg
sidebar:
  order: 0
---

## What is Embucket?

Embucket is a **Snowflake-compatible OLAP lakehouse platform** that provides the SQL interface and tooling ecosystem you already know from Snowflake, but built on an open lakehouse architecture. Think of it as your "Snowflake bring-your-own-cloud option" – delivering the same familiar developer experience while keeping your data in open formats and your infrastructure under your complete control.

Built for **data engineers, database operators, and data analysts**, Embucket eliminates the complexity of traditional data platforms by combining two key innovations: a statically-linked single binary for effortless deployment, and a zero-disk architecture that uses object storage as the foundation for both data and metadata.

## Why Embucket?

Modern data teams face an impossible choice: accept vendor lock-in with managed cloud warehouses, or embrace the operational complexity of building their own lakehouse stack. Embucket solves this by focusing on three core principles:

**Radical Simplicity**: In a world of complex, dependency-heavy data stacks, Embucket's single-binary architecture is a game-changer. Go from download to query in minutes – no external databases, no cluster management, no operational overhead. This statically-linked binary contains everything you need, with a zero-disk architecture that uses only object storage for both data and metadata.

**Fanatical Snowflake Compatibility**: Leverage the vast ecosystem of tools built for Snowflake without any vendor lock-in. Connect your existing BI tools, dbt transformations, and data science workflows to Embucket with zero configuration changes. Write the exact SQL you're used to – we've implemented Snowflake's SQL grammar and REST semantics to ensure seamless compatibility.

**Open-Source Foundation**: Built on a foundation of proven, best-in-class open-source technologies that you can trust. Embucket leverages Apache DataFusion for lightning-fast query execution, Apache Iceberg for ACID transactions and schema evolution, and SlateDB for metadata management. This isn't a proprietary black box – it's a well-integrated component in the modern open-source data stack, with active contributions back to these upstream projects.

In short: Embucket gives you the interface you already know from Snowflake while keeping your data in open formats and your compute bill firmly under your control.

## Key Features

### Stateless compute

Servers keep no local state; durability and metadata are handled by object storage and the Iceberg catalog. This means upgrades, scaling, and failover are trivial.

### Query-per-node architecture

Each node is both coordinator and executor, eliminating a single point of failure. Add or remove nodes elastically to handle load spikes.

### Snowflake-first compatibility

Instead of inventing a new dialect, Embucket implements Snowflake’s SQL grammar and REST semantics so you can reuse drivers, SDKs, and workflows with minimal changes.

### Apache 2.0 license

Open, forkable, and community-driven.

## Architecture

```mermaid
flowchart LR
  subgraph Clients
    A[Snowflake Drivers / dbt / BI] --SQL over HTTPS--> B(Embucket API)
  end
  subgraph Embucket Node
    B --> C[Query Engine<br>(Apache DataFusion)]
    B --> D[Iceberg Catalog Adapter]
  end
  C -->|Parquet| E[S3 / GCS / Azure]
  D -->|REST| F[Iceberg<br>Catalog]
```
