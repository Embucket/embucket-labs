# core-executor

The core query execution engine for Embucket, built on DataFusion. It handles query parsing, planning, optimization, and execution across various data sources.

## Purpose

This crate is central to Embucket's data processing capabilities. It leverages Apache DataFusion to execute SQL queries against configured catalogs and data sources.

## Async Query Execution
Query submitted asynchronously with fn `submit_query` returns AsyncQueryHandle which can be used with fn `wait_submitted_query_result` to consume query result. Underneath that two functions use `tokio::oneshot::channel` to communicate with each other.

## Historical Query Result
Multiple listeners can request result of running query with fn `wait_historical_query_result`. In opposite to polling which just returns status, it returns historical result if query isn't running anymore, or will wait for it to finish. As soon as query is finished and stored in history, listeners will get query result.
To make this happen `tokio::watch::channel` is used underneath for notifying listeners about query result status changes.

## Abort Query
Query can be aborted with fn `abort_query`.
Also SQL interface exposes `SYSTEM$CANCEL_QUERY` udf for aborting query by query UUID provided as string.
``` sql
SELECT SYSTEM$CANCEL_QUERY('123e4567-e89b-12d3-a456-426614174000');
```

## Running Queries Registry
`struct RunningQueriesRegistry` used for storing running queries info like cancellation token and Sender / Recever handles of watch channel. `trait RunningQueries` provides some interface for managing. This interface is used by ExecutionService and by `SYSTEM$CANCEL_QUERY` udf for queries aborting.