# core-metastore

Core library responsible for the abstraction and interaction with the underlying metadata storage system. Defines data models and traits for metastore operations.

## Purpose

This crate provides a consistent way for other Embucket components to access and manipulate metadata about catalogs, schemas, tables, and other entities, abstracting the specific storage backend.

### Using Sqlite based Metastore with Diesel ORM

Find Diesel config in `diesel.toml` file. 

To run migrations use:

```bash
# run migrations (for first time it creates database tables)
    diesel migration run --database-url "file:sqlite_data/metastore.db"

# get diesel schema (for development)
diesel print-schema --database-url "file:sqlite_data/metastore.db"
```


