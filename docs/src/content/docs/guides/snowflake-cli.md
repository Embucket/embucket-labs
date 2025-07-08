---
title: Using snowflake-cli to connect to Embucket
description: Learn how to use snowflake-cli to connect to Embucket
---

In this guide we will cover how to use snowflake-cli to connect to Embucket.

## Install snowflake-cli

You can install snowflake-cli using the following command:

```bash
pip install snowflake-cli
```

Once installed you need to setup your snowflake connection. This is done with `snow connection add`.

```bash
snow connection add --connection-name local --account test --user embucket --password embucket --host localhost --port 3000 --account xxx.us-east-2.aws --region us-east-2
```

This will create a connection named `local` that you can use to connect to Embucket. Since Embucket is running locally, we use `localhost` as the host and `3000` as the port. We also need to specify protocol to http since we are not using https. Find config file used by snowflake-cli with `snow --info`. On MacOS it might be located at `/Users/XXXXX/Library/Application Support/snowflake/config.toml`.

Update the file and add `protocol = "http"` under `[connections.local]`:

```
[connections.local]
host = "localhost"
region = "us-east-2"
port = 3000
protocol = "http"
database = "embucket"
schema = "public"
warehouse = "xxx"
password = "embucket"
account = "xxx.us-east-2.aws"
user = "embucket"
```

Now you can use `snow` to connect to Embucket. Make sure Embucket is running:

```bash
docker run --name embucket --rm -p 8080:8080 -p 3000:3000 embucket/embucket
```

And then connect to it:

```bash
snow sql -c local
```

You will see snowflake CLI interactive shell. You can now run SQL queries against Embucket. Let's create a table and insert some data:

```sql
CREATE TABLE employees (id INT, name STRING, department STRING, salary DECIMAL(10,2));
INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 95000.00), (2, 'Bob Smith', 'Marketing', 75000.00), (3, 'Carol Davis', 'Engineering', 98000.00);
```

And now let's query it:

```sql
SELECT * FROM employees;
```

You should see the data you inserted.

```

 > SELECT *  FROM employees;
+---------------------------------------------+
| id | name          | department  | salary   |
|----+---------------+-------------+----------|
| 1  | Alice Johnson | Engineering | 95000.00 |
| 2  | Bob Smith     | Marketing   | 75000.00 |
| 3  | Carol Davis   | Engineering | 98000.00 |
+---------------------------------------------+
```
