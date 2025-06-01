# df-catalog

Implements DataFusion's `CatalogProvider` and related traits, enabling the query engine to discover and interact with schemas, tables, and views managed by Embucket's metastore and external catalog sources like Iceberg.

## Purpose

This crate acts as a bridge between Embucket's metadata management (`core-metastore`) and the DataFusion query engine (`core-executor`), allowing DataFusion to understand the structure of data accessible via Embucket.
