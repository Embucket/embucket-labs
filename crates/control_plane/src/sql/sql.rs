use arrow::array::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::common::Result;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{ObjectName, Statement};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rust::catalog::create::CreateTable;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::identifier::Identifier;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::StructType;

pub async fn sql_query(ctx: SessionContext, query: &String, warehouse_name: &String) -> Result<Vec<RecordBatch>> {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect.as_str();
    let statement = state.sql_to_statement(query, dialect)?;

    if let DFStatement::Statement(s) = statement {
        if let Statement::CreateTable { or_replace, temporary, external, global,
            if_not_exists, transient, name, columns,
            constraints, hive_distribution, hive_formats,
            table_properties, with_options, file_format,
            location, query, without_rowid, like,
            clone, engine, comment, auto_increment_offset,
            default_charset, collation, on_commit,
            on_cluster, order_by, partition_by,
            cluster_by, options, strict } = *s {

            // Split table that needs to be created into parts
            let new_table_full_name = name.to_string();
            let new_table_wh_id = name.0[0].clone();
            let new_table_db = name.0[1].clone();
            let new_table_name = name.0[2].clone();

            // Replace the name of table that needs creation (for ex. "warehouse"."database"."table" -> "table")
            // And run the query - this will create an InMemory table
            let modified_statement = Statement::CreateTable { or_replace, temporary, external, global, if_not_exists,
                transient, columns, constraints, hive_distribution, hive_formats, table_properties, with_options,
                file_format, query, without_rowid, like, clone, engine, comment, auto_increment_offset,
                default_charset, collation, on_commit, on_cluster, order_by, partition_by, cluster_by, options, strict,
                location: location.clone(),
                name: ObjectName { 0: vec![new_table_name.clone()] }};
            let updated_query = modified_statement.to_string();
            ctx.sql(&updated_query).await.unwrap().collect().await.unwrap();

            // Get schema of new table
            let plan = ctx.state().create_logical_plan(&updated_query).await.unwrap();
            let schema = Schema::builder()
                .with_schema_id(0)
                .with_identifier_field_ids(vec![])
                .with_fields(
                    StructType::try_from(plan.schema().as_arrow()).unwrap()
                )
                .build()
                .unwrap();

            // Check if it already exists, if it is - drop it
            // For now we behave as CREATE OR REPLACE
            // TODO support CREATE without REPLACE
            let catalog = ctx.catalog(warehouse_name).unwrap();
            let iceberg_catalog = catalog.as_any().downcast_ref::<IcebergCatalog>().unwrap();
            let rest_catalog = iceberg_catalog.catalog();
            let new_table_ident = Identifier::new(&[new_table_db.value], &*new_table_name.value);
            match rest_catalog.tabular_exists(&new_table_ident).await {
                Ok(true) => {
                    rest_catalog.drop_table(&new_table_ident).await.unwrap();
                }
                Ok(false) => {}
                Err(_) => {}
            };

            // Create new table
            rest_catalog.create_table(new_table_ident.clone(), CreateTable {
                name: new_table_name.value.clone(),
                location,
                schema,
                partition_spec: None,
                write_order: None,
                stage_create: None,
                properties: None,
            }).await.unwrap();

            // Copy data from InMemory table to created table
            let insert_query = format!("INSERT INTO {new_table_full_name} SELECT * FROM {new_table_name}");
            let result = ctx.sql(&insert_query).await.unwrap().collect().await.unwrap();

            // Drop InMemory table
            let drop_query = format!("DROP TABLE {new_table_name}");
            ctx.sql(&drop_query).await.unwrap().collect().await.unwrap();

            return Ok(result);
        }
    }

    ctx.sql(query).await?.collect().await
}
