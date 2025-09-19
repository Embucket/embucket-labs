# api-snowflake-rest

Provides a REST API compatible with the Snowflake V1 SQL API, allowing Snowflake SDK clients and tooling to connect and interact with Embucket.

## Purpose

This crate allows tools and applications that use the Snowflake client SDKs to connect to Embucket as if it were a Snowflake instance, enabling query execution and other interactions via the Snowflake SQL API.

## `snow sql` programmatic API
`snow_sql` function provides a programmatic API to interact with the server
via Snowflake REST API in similar way as `snow sql` command line tool does.

There is also a `sql_test` macro which is a wrapper around `snow_sql` function. Test suite uses this helper macro to run SQLs like it was executed from `snow sql`.

## Testing
Snowflake REST API allows to build only tests, to be tested against external Embucket server.
In some cases it can help with reducing development cycle, in case if changes are not directly related to project itself. At the moment only following quick tests are supported: `test_rest_quick_sqls.rs`.

Use following command to run quick tests: 
``` bash
cargo test-rest
```

## Features
In order to provide coditional compilation for quick tests, this crate exposes features:
- `default-server` - enabled by default, enables test code for CI/CD tests, workspace tests.
- `external-server` - for quick tests, depends on external server, therefore re/compiles fast.
