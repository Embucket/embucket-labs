---
title: Quick Start
description: Get up and running with Embucket in under 5 minutes. From download to first query with zero dependencies.
---

Get Embucket running and execute your first query in under 5 minutes. This guide leverages Embucket's single-binary simplicity and .env configuration to get you to your first "magic moment" as quickly as possible.

## Step 1: Start Embucket

Launch the Embucket server:

```bash
docker run --name embucket --rm -p 8080:8080 -p 3000:3000 embucket/embucket
```

**That's it!** No external dependencies, no databases to install, no complex configuration. Embucket's zero-disk architecture means everything runs in-memory for this quick start.

You should see output similar to:

```
{"timestamp":"2025-07-01T15:35:05.687708Z","level":"INFO","fields":{"message":"Listening on http://0.0.0.0:8080"},"target":"embucketd"}
{"timestamp":"2025-07-01T15:35:05.687807Z","level":"INFO","fields":{"message":"Listening on http://0.0.0.0:3000"},"target":"embucketd"}
```

Default configuration for the provided docker image uses **file storage** for metadata and data. This is not recommended for production use. For more configuration options, see the [configuration](/essentials/configuration) guide.

Embucket is now running! The server provides:

- Snowflake-compatible REST API at `http://127.0.0.1:3000`
- Iceberg catalog REST API at `http://127.0.0.1:3000/catalog`
- Web UI Dashboard at `http://127.0.0.1:8080`

## Step 2: Execute your first query

Open browser to `http://127.0.0.1:8080` to explore Embucket's dashboard, query editor, and data catalog. Its UX should be very similar to Snowflake (use `embucket` as the username and `embucket` as the password):

![Embucket Web UI](/assets/quick-start-ui.png)

Let's create a table and insert some data. You can do this in the query editor in the web UI or via snowflake-cli. Open the browser to `http://127.0.0.1:8080` and navigate to the query editor. Copy and paste the following SQL:

```sql
CREATE TABLE employees (id INT, name STRING, department STRING, salary DECIMAL(10,2));
INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 95000.00), (2, 'Bob Smith', 'Marketing', 75000.00), (3, 'Carol Davis', 'Engineering', 98000.00);
```

### Query your data:

```sql
SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department ORDER BY avg_salary DESC
```

Behind the hood, Embucket uses Apache Iceberg to store your data and SlateDB to store your metadata.
