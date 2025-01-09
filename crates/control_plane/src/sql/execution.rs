#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

use super::error::{self as sql_error, SQLResult};
use crate::models::{created_entity_response, Warehouse};
use crate::sql::context::CustomContextProvider;
use crate::sql::functions::convert_timezone::ConvertTimezoneFunc;
use crate::sql::functions::date_add::DateAddFunc;
use crate::sql::functions::greatest::GreatestFunc;
use crate::sql::functions::least::LeastFunc;
use crate::sql::functions::parse_json::ParseJsonFunc;
use crate::sql::planner::ExtendedSqlToRel;
use arrow::array::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::catalog_common::information_schema::InformationSchemaProvider;
use datafusion::catalog_common::{ResolvedTableReference, TableReference};
use datafusion::common::plan_datafusion_err;
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::datasource::default_table_source::provider_as_source;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::sqlparser::ast::Insert;
use datafusion::logical_expr::{
    CreateExternalTable as PlanCreateExternalTable, DdlStatement, LogicalPlan, ScalarUDF,
};
use datafusion::sql::parser::{CreateExternalTable, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    CreateTable as CreateTableStatement, Expr, Ident, ObjectName, Query, SchemaName, Statement,
    TableFactor, TableWithJoins,
};
use datafusion_functions_json::register_all;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use datafusion_iceberg::planner::iceberg_transform;
use iceberg_rust::spec::namespace::Namespace;
use snafu::ResultExt;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SqlExecutor {
    ctx: SessionContext,
}

impl SqlExecutor {
    pub fn new(mut ctx: SessionContext) -> SQLResult<Self> {
        ctx.register_udf(ScalarUDF::from(ParseJsonFunc::new()));
        ctx.register_udf(ScalarUDF::from(DateAddFunc::new()));
        ctx.register_udf(ScalarUDF::from(LeastFunc::new()));
        ctx.register_udf(ScalarUDF::from(GreatestFunc::new()));
        ctx.register_udf(ScalarUDF::from(ConvertTimezoneFunc::new()));
        register_all(&mut ctx).context(sql_error::RegisterUDFSnafu)?;
        Ok(Self { ctx })
    }

    pub async fn query(&self, query: &str, warehouse: &Warehouse) -> SQLResult<Vec<RecordBatch>> {
        let warehouse_name = &*warehouse.name;
        let state = self.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        // Update query to use custom JSON functions
        let query = self.preprocess_query(query);
        let statement = state
            .sql_to_statement(&query, dialect)
            .context(super::error::DataFusionSnafu)?;
        // statement = self.update_statement_references(statement, warehouse_name);
        // query = statement.to_string();

        if let DFStatement::Statement(s) = statement {
            match *s {
                Statement::CreateTable { .. } => {
                    return Box::pin(self.create_table_query(*s, warehouse)).await;
                }
                Statement::CreateSchema { schema_name, .. } => {
                    return self.create_schema(schema_name, warehouse_name).await;
                }
                Statement::ShowVariable { .. } | Statement::Drop { .. } => {
                    return Box::pin(self.execute_with_custom_plan(&query, warehouse_name)).await;
                }
                Statement::Query { .. } => {
                    return self.execute_with_custom_plan(&query, warehouse_name).await;
                }
                _ => {}
            }
        }
        self.ctx
            .sql(&query)
            .await
            .context(super::error::DataFusionSnafu)?
            .collect()
            .await
            .context(super::error::DataFusionSnafu)
    }

    /// .
    ///
    /// # Panics
    ///
    /// Panics if .
    #[must_use]
    #[allow(clippy::unwrap_used)]
    pub fn preprocess_query(&self, query: &str) -> String {
        // Replace field[0].subfield -> json_get(json_get(field, 0), 'subfield')
        // TODO: This regex should be a static allocation
        let re = regex::Regex::new(r"(\w+)\[(\d+)]\.(\w+)").unwrap();
        let date_add = regex::Regex::new(r"(date|time|timestamp)(_?add)\(\s*([a-zA-Z]+),").unwrap();

        let query = re
            .replace_all(query, "json_get(json_get($1, $2), '$3')")
            .to_string();
        let query = date_add.replace_all(&query, "$1$2('$3',").to_string();
        // TODO implement alter session logic
        query.replace(
            "alter session set query_tag = 'snowplow_dbt'",
            "SHOW session",
        )
    }

    #[allow(clippy::redundant_else, clippy::too_many_lines)]
    pub async fn create_table_query(
        &self,
        statement: Statement,
        warehouse: &Warehouse,
    ) -> SQLResult<Vec<RecordBatch>> {
        if let Statement::CreateTable(create_table_statement) = statement {
            let warehouse_name = &*warehouse.name;
            let mut new_table_full_name = create_table_statement.name.to_string();
            let mut ident = create_table_statement.name.0;
            if !new_table_full_name.starts_with(warehouse_name) {
                new_table_full_name = format!("{warehouse_name}.{new_table_full_name}");
                ident.insert(0, Ident::new(warehouse_name));
            }
            let _new_table_wh_id = ident[0].clone();
            let new_table_db = &ident[1..ident.len() - 1]
                .iter()
                .map(|v| v.value.clone())
                .collect::<Vec<String>>()
                .join(".");
            #[allow(clippy::unwrap_used)]
            let new_table_name = ident.last().unwrap();
            let new_table_ref = TableReference::full(
                warehouse_name,
                new_table_db.to_string(),
                new_table_name.value.clone(),
            );
            let location = warehouse.location.clone();

            // Replace the name of table that needs creation (for ex. "warehouse"."database"."table" -> "table")
            // And run the query - this will create an InMemory table
            let modified_statement = CreateTableStatement {
                name: ObjectName(vec![new_table_name.clone()]),
                transient: false,
                ..create_table_statement
            };
            // Create InMemory table since external tables with "AS SELECT" are not supported
            let updated_query = modified_statement.to_string();
            let plan = self
                .get_custom_logical_plan(&updated_query, warehouse_name)
                .await?;
            self.ctx
                .execute_logical_plan(plan.clone())
                .await
                .context(super::error::DataFusionSnafu)?
                .collect()
                .await
                .context(super::error::DataFusionSnafu)?;

            // Create External table
            if let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(table)) = plan {
                let external_table_plan =
                    LogicalPlan::Ddl(DdlStatement::CreateExternalTable(PlanCreateExternalTable {
                        schema: table.input.schema().clone(),
                        name: new_table_ref,
                        location,                         // Specify the external location
                        file_type: "ICEBERG".to_string(), // Specify the file type
                        table_partition_cols: vec![],     // Specify partition columns if any
                        if_not_exists: table.if_not_exists,
                        temporary: table.temporary,
                        definition: None, // Specify the SQL definition if available
                        order_exprs: vec![], // Specify order expressions if any
                        unbounded: false, // Specify if the table is unbounded
                        options: HashMap::new(), // Specify table-specific options if any
                        constraints: table.constraints.clone(),
                        column_defaults: HashMap::new(),
                    }));
                let transformed = external_table_plan
                    .transform(iceberg_transform)
                    .data()
                    .context(sql_error::DataFusionSnafu)?;
                self.ctx
                    .execute_logical_plan(transformed)
                    .await
                    .context(sql_error::DataFusionSnafu)?
                    .collect()
                    .await
                    .context(sql_error::DataFusionSnafu)?;
            }

            // Insert data to External table
            let insert_query =
                format!("INSERT INTO {new_table_full_name} SELECT * FROM {new_table_name}");
            self.execute_with_custom_plan(&insert_query, warehouse_name)
                .await?;

            // Drop InMemory table
            let drop_query = format!("DROP TABLE {new_table_name}");
            self.ctx
                .sql(&drop_query)
                .await
                .context(super::error::DataFusionSnafu)?
                .collect()
                .await
                .context(super::error::DataFusionSnafu)?;

            created_entity_response().context(sql_error::ArrowSnafu)
        } else {
            Err(super::error::SQLError::DataFusion {
                source: datafusion::error::DataFusionError::NotImplemented(
                    "Only CREATE TABLE statements are supported".to_string(),
                ),
            })
        }
    }

    pub async fn create_schema(
        &self,
        name: SchemaName,
        warehouse_name: &str,
    ) -> SQLResult<Vec<RecordBatch>> {
        match name {
            SchemaName::Simple(schema_name) => {
                //println!("Creating simple schema: {:?}", schema_name);
                // TODO: Abstract the Iceberg catalog
                let catalog = self.ctx.catalog(warehouse_name).ok_or(
                    sql_error::SQLError::WarehouseNotFound {
                        name: warehouse_name.to_string(),
                    },
                )?;
                let iceberg_catalog = catalog.as_any().downcast_ref::<IcebergCatalog>().ok_or(
                    sql_error::SQLError::IcebergCatalogNotFound {
                        warehouse_name: warehouse_name.to_string(),
                    },
                )?;
                let rest_catalog = iceberg_catalog.catalog();
                let namespace_vec: Vec<String> = schema_name
                    .0
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect();
                let single_layer_namespace = vec![namespace_vec.join(".")];
                #[allow(clippy::unwrap_used)]
                let namespace = Namespace::try_new(&single_layer_namespace).unwrap();
                // Why are we checking if namespace exists as a single layer and then creating it?
                // There are multiple possible Error scenarios here that are not handled
                if rest_catalog.load_namespace(&namespace).await.is_err() {
                    #[allow(clippy::unwrap_used)]
                    let namespace = Namespace::try_new(&namespace_vec).unwrap();
                    rest_catalog
                        .create_namespace(&namespace, None)
                        .await
                        .context(sql_error::IcebergSnafu)?;
                }
            }
            _ => {
                return Err(super::error::SQLError::DataFusion {
                    source: datafusion::error::DataFusionError::NotImplemented(
                        "Only simple schema names are supported".to_string(),
                    ),
                });
            }
        }
        created_entity_response().context(super::error::ArrowSnafu)
    }

    pub async fn get_custom_logical_plan(
        &self,
        query: &str,
        warehouse_name: &str,
    ) -> SQLResult<LogicalPlan> {
        let state = self.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        let mut statement = state
            .sql_to_statement(query, dialect)
            .context(super::error::DataFusionSnafu)?;
        //println!("raw query: {:?}", statement.to_string());
        statement = self.update_statement_references(statement, warehouse_name);
        //println!("modified query: {:?}", statement.to_string());

        if let DFStatement::Statement(s) = statement.clone() {
            let mut ctx_provider = CustomContextProvider {
                state: &state,
                tables: HashMap::new(),
            };
            let references = state
                .resolve_table_references(&statement)
                .context(super::error::DataFusionSnafu)?;
            //println!("References: {:?}", references);
            for reference in references {
                let resolved = self.resolve_table_ref(reference);
                if let Entry::Vacant(v) = ctx_provider.tables.entry(resolved.to_string()) {
                    if let Ok(schema) = self.schema_for_ref(resolved.clone()) {
                        if let Some(table) = schema
                            .table(&resolved.table)
                            .await
                            .context(super::error::DataFusionSnafu)?
                        {
                            v.insert(provider_as_source(table));
                        }
                    }
                }
            }
            #[allow(clippy::unwrap_used)]
            // Unwraps are allowed here because we are sure that objects exists
            for catalog in self.ctx.state().catalog_list().catalog_names() {
                let provider = self.ctx.state().catalog_list().catalog(&catalog).unwrap();
                for schema in provider.schema_names() {
                    for table in provider.schema(&schema).unwrap().table_names() {
                        let table_source = provider
                            .schema(&schema)
                            .unwrap()
                            .table(&table)
                            .await
                            .context(super::error::DataFusionSnafu)?
                            .ok_or(super::error::SQLError::TableProviderNotFound {
                                table_name: table.clone(),
                            })?;
                        ctx_provider.tables.insert(
                            format!("{catalog}.{schema}.{table}"),
                            provider_as_source(table_source),
                        );
                    }
                }
            }
            // println!("Tables: {:?}", ctx_provider.tables.keys());
            let planner = ExtendedSqlToRel::new(&ctx_provider);
            planner
                .sql_statement_to_plan(*s)
                .context(super::error::DataFusionSnafu)
        } else {
            Err(super::error::SQLError::DataFusion {
                source: datafusion::error::DataFusionError::NotImplemented(
                    "Only SQL statements are supported".to_string(),
                ),
            })
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
    ) -> SQLResult<Arc<dyn SchemaProvider>> {
        let state = self.ctx.state();
        let resolved_ref = self.resolve_table_ref(table_ref);
        if state.config().information_schema() && *resolved_ref.schema == *"information_schema" {
            return Ok(Arc::new(InformationSchemaProvider::new(
                state.catalog_list().clone(),
            )));
        }

        // Need better error handling here instead of just DF errors
        state
            .catalog_list()
            .catalog(&resolved_ref.catalog)
            .ok_or_else(|| super::error::SQLError::DataFusion {
                source: plan_datafusion_err!("failed to resolve catalog: {}", resolved_ref.catalog),
            })?
            .schema(&resolved_ref.schema)
            .ok_or_else(|| super::error::SQLError::DataFusion {
                source: plan_datafusion_err!("failed to resolve schema: {}", resolved_ref.schema),
            })
    }

    pub async fn execute_with_custom_plan(
        &self,
        query: &str,
        warehouse_name: &str,
    ) -> SQLResult<Vec<RecordBatch>> {
        let plan = self.get_custom_logical_plan(query, warehouse_name).await?;
        self.ctx
            .execute_logical_plan(plan)
            .await
            .context(super::error::DataFusionSnafu)?
            .collect()
            .await
            .context(super::error::DataFusionSnafu)
    }

    #[must_use]
    pub fn update_statement_references(
        &self,
        statement: DFStatement,
        warehouse_name: &str,
    ) -> DFStatement {
        match statement.clone() {
            DFStatement::CreateExternalTable(create_external) => {
                let table_name =
                    self.compress_database_name(create_external.name.0, warehouse_name);
                let modified_statement = CreateExternalTable {
                    name: ObjectName(table_name),
                    ..create_external
                };
                DFStatement::CreateExternalTable(modified_statement)
            }
            DFStatement::Statement(s) => match *s {
                Statement::Insert(insert_statement) => {
                    let table_name =
                        self.compress_database_name(insert_statement.table_name.0, warehouse_name);
                    let modified_statement = Insert {
                        table_name: ObjectName(table_name),
                        ..insert_statement
                    };
                    DFStatement::Statement(Box::new(Statement::Insert(modified_statement)))
                }
                Statement::Drop {
                    object_type,
                    if_exists,
                    names,
                    cascade,
                    restrict,
                    purge,
                    temporary,
                } => {
                    let names = self.compress_database_name(names[0].clone().0, warehouse_name);
                    let modified_statement = Statement::Drop {
                        object_type,
                        if_exists,
                        names: vec![ObjectName(names)],
                        cascade,
                        restrict,
                        purge,
                        temporary,
                    };
                    DFStatement::Statement(Box::new(modified_statement))
                }
                Statement::Query(mut query) => {
                    self.update_tables_in_query(query.as_mut(), warehouse_name);
                    DFStatement::Statement(Box::new(Statement::Query(query)))
                }
                Statement::CreateTable(create_table_statement) => {
                    if create_table_statement.query.is_some() {
                        #[allow(clippy::unwrap_used)]
                        let mut query = create_table_statement.query.unwrap();
                        self.update_tables_in_query(&mut query, warehouse_name);
                        let modified_statement = CreateTableStatement {
                            query: Some(query),
                            ..create_table_statement
                        };
                        DFStatement::Statement(Box::new(Statement::CreateTable(modified_statement)))
                    } else {
                        statement
                    }
                }
                _ => statement,
            },
            _ => statement,
        }
    }

    // Combine database name identifiers into single Ident
    #[must_use]
    pub fn compress_database_name(
        &self,
        mut table_name: Vec<Ident>,
        warehouse_name: &str,
    ) -> Vec<Ident> {
        if !warehouse_name.is_empty()
            && !table_name.starts_with(&[Ident::new(warehouse_name)])
            && table_name.len() > 1
        {
            table_name.insert(0, Ident::new(warehouse_name));
        }
        #[allow(clippy::unwrap_used)]
        if table_name.len() > 3 {
            let new_table_db = &table_name[1..table_name.len() - 1]
                .iter()
                .map(|v| v.value.clone())
                .collect::<Vec<String>>()
                .join(".");

            table_name = vec![
                table_name[0].clone(),
                Ident::new(new_table_db),
                table_name.last().unwrap().clone(),
            ];
        }
        table_name
    }

    fn update_tables_in_query(&self, query: &mut Query, warehouse_name: &str) {
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                self.update_tables_in_query(&mut cte.query, warehouse_name);
            }
        }

        match query.body.as_mut() {
            datafusion::sql::sqlparser::ast::SetExpr::Select(select) => {
                for table_with_joins in &mut select.from {
                    self.update_tables_in_table_with_joins(table_with_joins, warehouse_name);
                }

                if let Some(expr) = &mut select.selection {
                    self.update_tables_in_expr(expr, warehouse_name);
                }
            }
            datafusion::sql::sqlparser::ast::SetExpr::Query(q) => {
                self.update_tables_in_query(q, warehouse_name);
            }
            _ => {}
        }
    }

    fn update_tables_in_expr(&self, expr: &mut Expr, warehouse_name: &str) {
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                self.update_tables_in_expr(left, warehouse_name);
                self.update_tables_in_expr(right, warehouse_name);
            }
            Expr::Subquery(q) => {
                self.update_tables_in_query(q, warehouse_name);
            }
            Expr::Exists { subquery, .. } => {
                self.update_tables_in_query(subquery, warehouse_name);
            }
            _ => {}
        }
    }

    fn update_tables_in_table_with_joins(
        &self,
        table_with_joins: &mut TableWithJoins,
        warehouse_name: &str,
    ) {
        self.update_tables_in_table_factor(&mut table_with_joins.relation, warehouse_name);

        for join in &mut table_with_joins.joins {
            self.update_tables_in_table_factor(&mut join.relation, warehouse_name);
        }
    }

    fn update_tables_in_table_factor(&self, table_factor: &mut TableFactor, warehouse_name: &str) {
        match table_factor {
            TableFactor::Table { name, .. } => {
                let compressed_name = self.compress_database_name(name.0.clone(), warehouse_name);
                *name = ObjectName(compressed_name);
            }
            TableFactor::Derived { subquery, .. } => {
                self.update_tables_in_query(subquery, warehouse_name);
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.update_tables_in_table_with_joins(table_with_joins, warehouse_name);
            }
            _ => {}
        }
    }
}
