# Embucket

**Run Snowflake SQL dialect on your data lake in 30 seconds. Zero dependencies.**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![SQL Logic Test Coverage](https://raw.githubusercontent.com/Embucket/embucket/assets/assets/badge.svg)](test/README.md)
[![dbt Gitlab run results](https://raw.githubusercontent.com/Embucket/embucket/assets_dbt/assets_dbt/dbt_success_badge.svg)](test/dbt_integration_tests/dbt-gitlab/README.md)

## Quick start

Start Embucket and run your first query in 30 seconds:

```bash
docker run --name embucket --rm -p 8080:8080 -p 3000:3000 embucket/embucket
```

Open [localhost:8080](http://localhost:8080)—login: `embucket`/`embucket`—and run:

```sql
CREATE TABLE sales (id INT, product STRING, revenue DECIMAL(10,2));
INSERT INTO sales VALUES (1, 'Widget A', 1250.00), (2, 'Widget B', 899.50);
SELECT product, revenue FROM sales WHERE revenue > 1000;
```

**Done.** You just ran Snowflake SQL dialect on Apache Iceberg tables with zero configuration.

## What just happened?

Embucket provides a **single binary** that gives you a **wire-compatible Snowflake replacement**:

- **Snowflake SQL dialect and API**: Use your existing queries, dbt projects, and BI tools
- **Apache Iceberg storage**: Your data stays in open formats on object storage  
- **Zero dependencies**: No databases, no clusters, no configuration files
- **Query-per-node**: Each instance handles complete queries independently

Perfect for teams who want Snowflake's simplicity with bring-your-own-cloud control.

## Architecture

![Embucket Architecture](architecture.png)

**Zero-disk lakehouse**: an architectural approach where all data and metadata live in object storage rather than on compute nodes. Nodes stay stateless and replaceable.

Built on proven open source:
- [Apache DataFusion](https://datafusion.apache.org/) for SQL execution
- [Apache Iceberg](https://iceberg.apache.org/) for ACID transactions  
- [SlateDB](https://slatedb.io/) for metadata management

## Why Embucket?

**Escape the dilemma**: choose between cloud provider lakehouses (Redshift, BigQuery) or operational complexity (do-it-yourself lakehouse).

- **Radical simplicity** - Single binary deployment  
- **Snowflake SQL dialect compatibility** - Works with your existing tools  
- **Open data** - Apache Iceberg format, no lock-in  
- **Horizontal scaling** - Add nodes for more throughput  
- **Zero operations** - No external dependencies to manage

## Next steps

**Ready for more?** Check out the comprehensive documentation:

[Quick start](https://docs.embucket.com/essentials/quick-start/) - Detailed setup and first queries  
[Architecture](https://docs.embucket.com/essentials/architecture/) - How the zero-disk lakehouse works  
[Configuration](https://docs.embucket.com/essentials/configuration/) - Production deployment options  
[dbt Integration](https://docs.embucket.com/guides/dbt-snowplow/) - Run existing dbt projects  

**From source:**
```bash
git clone https://github.com/Embucket/embucket.git
cd embucket && cargo build
./target/debug/embucketd
```

## Contributing  

Contributions welcome. To get involved:  

1. **Fork** the repository on GitHub  
2. **Create** a new branch for your feature or bug fix  
3. **Submit** a pull request with a detailed description  

For more details, see [CONTRIBUTING.md](CONTRIBUTING.md).  

## License  

This project uses the **Apache 2.0 License**. See [LICENSE](LICENSE) for details.  

