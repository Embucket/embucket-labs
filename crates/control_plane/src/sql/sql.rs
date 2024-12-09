use crate::sql::context::CustomContextProvider;
use crate::sql::functions::parse_json::ParseJsonFunc;
use crate::sql::planner::ExtendedSqlToRel;
use arrow::array::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::catalog_common::information_schema::InformationSchemaProvider;
use datafusion::catalog_common::{ResolvedTableReference, TableReference};
use datafusion::common::{plan_datafusion_err, Result};
use datafusion::datasource::default_table_source::provider_as_source;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{CreateTable as CreateTableStatement, ObjectName, Statement};
use datafusion_functions_json::register_all;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rust::catalog::create::CreateTable as CreateTableCatalog;
use iceberg_rust::spec::identifier::Identifier;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::StructType;
use regex;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SqlExecutor {
    ctx: SessionContext,
}

impl SqlExecutor {
    pub fn new(mut ctx: SessionContext) -> Self {
        ctx.register_udf(ScalarUDF::from(ParseJsonFunc::new()));
        register_all(&mut ctx).expect("Cannot register UDF JSON funcs");
        Self { ctx }
    }

    pub async fn query(&self, query: &String, warehouse_name: &String) -> Result<Vec<RecordBatch>> {
        let state = self.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        // Update query ti use custom JSON functions
        let query = self.preprocess_query(query);
        println!("Query: {}", query);
        let statement = state.sql_to_statement(&query, dialect)?;

        if let DFStatement::Statement(s) = statement {
            match *s {
                Statement::CreateTable { .. } => {
                    return self.create_table_query(*s, warehouse_name).await;
                }
                Statement::ShowVariable { .. } => {
                    return self.execute_with_custom_plan(&query).await;
                }
                _ => {}
            }
        }
        self.ctx.sql(&query).await?.collect().await
    }

    pub fn preprocess_query(&self, query: &String) -> String {
        // Replace field[0].subfield -> json_get(json_get(field, 0), 'subfield')
        let re = regex::Regex::new(r"(\w+)\[(\d+)]\.(\w+)").unwrap();
        re.replace_all(query, "json_get(json_get($1, $2), '$3')")
            .to_string()
    }

    pub async fn create_table_query(
        &self,
        statement: Statement,
        warehouse_name: &String,
    ) -> Result<Vec<RecordBatch>> {
        if let Statement::CreateTable(create_table_statement) = statement {
            let new_table_full_name = create_table_statement.name.to_string();
            let _new_table_wh_id = create_table_statement.name.0[0].clone();
            let new_table_db = create_table_statement.name.0[1].clone();
            let new_table_name = create_table_statement.name.0[2].clone();
            let location = create_table_statement.location.clone();

            // Replace the name of table that needs creation (for ex. "warehouse"."database"."table" -> "table")
            // And run the query - this will create an InMemory table
            let modified_statement = CreateTableStatement {
                name: ObjectName {
                    0: vec![new_table_name.clone()],
                },
                ..create_table_statement
            };
            let updated_query = modified_statement.to_string();
            self.execute_with_custom_plan(&updated_query).await?;

            // Get schema of new table
            let plan = self.get_custom_logical_plan(&updated_query).await?;
            let schema = Schema::builder()
                .with_schema_id(0)
                .with_identifier_field_ids(vec![])
                .with_fields(StructType::try_from(plan.schema().as_arrow()).unwrap())
                .build()
                .unwrap();

            // Check if it already exists, if it is - drop it
            // For now we behave as CREATE OR REPLACE
            // TODO support CREATE without REPLACE
            let catalog = self.ctx.catalog(warehouse_name).unwrap();
            let iceberg_catalog = catalog.as_any().downcast_ref::<IcebergCatalog>().unwrap();
            let rest_catalog = iceberg_catalog.catalog();
            let new_table_ident = Identifier::new(&[new_table_db.value], &new_table_name.value);
            match rest_catalog.tabular_exists(&new_table_ident).await {
                Ok(true) => {
                    rest_catalog.drop_table(&new_table_ident).await.unwrap();
                }
                Ok(false) => {}
                Err(_) => {}
            };

            // Create new table
            rest_catalog
                .create_table(
                    new_table_ident.clone(),
                    CreateTableCatalog {
                        name: new_table_name.value.clone(),
                        location,
                        schema,
                        partition_spec: None,
                        write_order: None,
                        stage_create: None,
                        properties: None,
                    },
                )
                .await
                .unwrap();

            // Copy data from InMemory table to created table
            let insert_query =
                format!("INSERT INTO {new_table_full_name} SELECT * FROM {new_table_name}");
            let result = self.ctx.sql(&insert_query).await?.collect().await?;

            // Drop InMemory table
            let drop_query = format!("DROP TABLE {new_table_name}");
            self.ctx.sql(&drop_query).await?.collect().await?;

            Ok(result)
        } else {
            Err(datafusion::error::DataFusionError::NotImplemented(
                "Only CREATE TABLE statements are supported".to_string(),
            ))
        }
    }

    pub async fn get_custom_logical_plan(&self, query: &String) -> Result<LogicalPlan> {
        let state = self.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        let statement = state.sql_to_statement(query, dialect)?;

        if let DFStatement::Statement(s) = statement.clone() {
            let references = state.resolve_table_references(&statement)?;

            let mut ctx_provider = CustomContextProvider {
                state: &state,
                tables: HashMap::new(),
            };

            for reference in references {
                let resolved = self.resolve_table_ref(reference);

                if let Entry::Vacant(v) = ctx_provider.tables.entry(resolved.to_string()) {
                    if let Ok(schema) = self.schema_for_ref(resolved.clone()) {
                        if let Some(table) = schema.table(&resolved.table).await? {
                            v.insert(provider_as_source(table));
                        }
                    }
                }
            }

            for catalog in self.ctx.state().catalog_list().catalog_names() {
                let provider = self.ctx.state().catalog_list().catalog(&catalog).unwrap();
                for schema in provider.schema_names() {
                    for table in provider.schema(&schema).unwrap().table_names() {
                        let table_source = provider
                            .schema(&schema)
                            .unwrap()
                            .table(&table)
                            .await?
                            .unwrap();
                        ctx_provider.tables.insert(
                            format!("{catalog}.{schema}.{table}"),
                            provider_as_source(table_source),
                        );
                    }
                }
            }
            let planner = ExtendedSqlToRel::new(&ctx_provider);
            planner.sql_statement_to_plan(*s)
        } else {
            Err(datafusion::error::DataFusionError::NotImplemented(
                "Only SQL statements are supported".to_string(),
            ))
        }
    }

    pub fn resolve_table_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> ResolvedTableReference {
        let catalog = &self.ctx.state().config_options().catalog.clone();
        table_ref
            .into()
            .resolve(&catalog.default_catalog, &catalog.default_schema)
    }

    pub fn schema_for_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let state = self.ctx.state();
        let resolved_ref = self.resolve_table_ref(table_ref);
        if state.config().information_schema() && *resolved_ref.schema == *"information_schema" {
            return Ok(Arc::new(InformationSchemaProvider::new(
                state.catalog_list().clone(),
            )));
        }

        state
            .catalog_list()
            .catalog(&resolved_ref.catalog)
            .ok_or_else(|| {
                plan_datafusion_err!("failed to resolve catalog: {}", resolved_ref.catalog)
            })?
            .schema(&resolved_ref.schema)
            .ok_or_else(|| {
                plan_datafusion_err!("failed to resolve schema: {}", resolved_ref.schema)
            })
    }

    pub async fn execute_with_custom_plan(&self, query: &String) -> Result<Vec<RecordBatch>> {
        let plan = self.get_custom_logical_plan(query).await?;
        self.ctx.execute_logical_plan(plan).await?.collect().await
    }
}
