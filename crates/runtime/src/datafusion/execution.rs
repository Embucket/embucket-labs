#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

use super::error::{self as ih_error, IcehutSQLError, IcehutSQLResult};
use crate::datafusion::functions::register_udfs;
use crate::datafusion::planner::ExtendedSqlToRel;
use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::datasource::default_table_source::provider_as_source;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionContextProvider;
use datafusion::logical_expr::sqlparser::ast::Insert;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::CsvReadOptions;
use datafusion::sql::parser::{CreateExternalTable, DFParser, Statement as DFStatement};
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{
    CreateTable as CreateTableStatement, Expr, Ident, ObjectName, Query, SchemaName, Statement,
    TableFactor, TableWithJoins,
};
use datafusion_common::{DataFusionError, TableReference};
use datafusion_functions_json::register_all;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use datafusion_iceberg::planner::iceberg_transform;
use iceberg_rust::catalog::create::CreateTable as CreateTableCatalog;
use iceberg_rust::spec::arrow::schema::new_fields_with_ids;
use iceberg_rust::spec::identifier::Identifier;
use iceberg_rust::spec::namespace::Namespace;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::StructType;
use object_store::aws::AmazonS3Builder;
use snafu::ResultExt;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, GroupByExpr, MergeAction, MergeClauseKind, MergeInsertKind, ObjectType,
    Query as AstQuery, Select, SelectItem,
};
use sqlparser::tokenizer::Span;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
pub struct TablePath {
    pub db: String,
    pub schema: String,
    pub table: String,
}

pub struct SqlExecutor {
    // ctx made public to register_catalog after creating SqlExecutor
    pub ctx: SessionContext,
    ident_normalizer: IdentNormalizer,
}

impl SqlExecutor {
    pub fn new(mut ctx: SessionContext) -> IcehutSQLResult<Self> {
        register_udfs(&mut ctx).context(ih_error::RegisterUDFSnafu)?;
        register_all(&mut ctx).context(ih_error::RegisterUDFSnafu)?;
        let enable_ident_normalization = ctx.enable_ident_normalization();
        Ok(Self {
            ctx,
            ident_normalizer: IdentNormalizer::new(enable_ident_normalization),
        })
    }

    pub fn parse_query(&self, query: &str) -> Result<DFStatement, DataFusionError> {
        let state = self.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        state.sql_to_statement(query, dialect)
    }

    #[tracing::instrument(level = "debug", skip(self), err, ret(level = tracing::Level::TRACE))]
    pub async fn query(
        &self,
        query: &str,
        warehouse_name: &str,
        warehouse_location: &str,
    ) -> IcehutSQLResult<Vec<RecordBatch>> {
        // Update query to use custom JSON functions
        let query = self.preprocess_query(query);
        let statement = self
            .parse_query(query.as_str())
            .context(super::error::DataFusionSnafu)?;
        // statement = self.update_statement_references(statement, warehouse_name);
        // query = statement.to_string();

        if let DFStatement::Statement(s) = statement {
            match *s {
                Statement::CreateTable { .. } => {
                    return Box::pin(self.create_table_query(*s, warehouse_name)).await;
                }
                Statement::CreateDatabase {
                    db_name,
                    if_not_exists,
                    ..
                } => {
                    return self
                        .create_database(warehouse_name, db_name, if_not_exists)
                        .await;
                }
                Statement::CreateSchema {
                    schema_name,
                    if_not_exists,
                } => {
                    return self
                        .create_schema(warehouse_name, schema_name, if_not_exists)
                        .await;
                }
                Statement::CreateStage { .. } => {
                    // We support only CSV uploads for now
                    return Box::pin(self.create_stage_query(*s, warehouse_name)).await;
                }
                Statement::CopyIntoSnowflake { .. } => {
                    return Box::pin(self.copy_into_snowflake_query(*s, warehouse_name)).await;
                }
                Statement::AlterTable { .. }
                | Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Insert { .. }
                | Statement::ShowSchemas { .. }
                | Statement::ShowVariable { .. }
                | Statement::Update { .. } => {
                    return Box::pin(self.execute_with_custom_plan(&query, warehouse_name)).await;
                }
                Statement::Query(mut subquery) => {
                    self.update_qualify_in_query(subquery.as_mut());
                    Self::update_table_result_scan_in_query(subquery.as_mut());
                    return Box::pin(
                        self.execute_with_custom_plan(&subquery.to_string(), warehouse_name),
                    )
                    .await;
                }
                Statement::Drop { .. } => {
                    return Box::pin(self.drop_query(&query, warehouse_name)).await;
                }
                Statement::Merge { .. } => {
                    return Box::pin(self.merge_query(*s, warehouse_name)).await;
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
    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub fn preprocess_query(&self, query: &str) -> String {
        // Replace field[0].subfield -> json_get(json_get(field, 0), 'subfield')
        // TODO: This regex should be a static allocation
        let re = regex::Regex::new(r"(\w+.\w+)\[(\d+)][:\.](\w+)").unwrap();
        let date_add =
            regex::Regex::new(r"(date|time|timestamp)(_?add|_?diff)\(\s*([a-zA-Z]+),").unwrap();

        let query = re
            .replace_all(query, "json_get(json_get($1, $2), '$3')")
            .to_string();
        let query = date_add.replace_all(&query, "$1$2('$3',").to_string();
        // TODO implement alter session logic
        query
            .replace(
                "alter session set query_tag = 'snowplow_dbt'",
                "SHOW session",
            )
            .replace("skip_header=1", "skip_header=TRUE")
            .replace("FROM @~/", "FROM ")
    }

    #[allow(clippy::redundant_else, clippy::too_many_lines)]
    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn create_table_query(
        &self,
        statement: Statement,
        warehouse_name: &str,
    ) -> IcehutSQLResult<Vec<RecordBatch>> {
        if let Statement::CreateTable(create_table_statement) = statement {
            let mut new_table_full_name = create_table_statement.name.to_string();
            let mut ident = create_table_statement.name.0;
            if !new_table_full_name.starts_with(warehouse_name) {
                new_table_full_name = format!("{warehouse_name}.{new_table_full_name}");
                ident.insert(0, Ident::new(warehouse_name));
            }
            let _new_table_wh_id = ident[0].clone();
            let new_table_db = &ident[1..ident.len() - 1];
            let new_table_name = ident
                .last()
                .ok_or(ih_error::IcehutSQLError::InvalidIdentifier {
                    ident: new_table_full_name.clone(),
                })?
                .clone();
            let location = create_table_statement.location.clone();

            // Replace the name of table that needs creation (for ex. "warehouse"."database"."table" -> "table")
            // And run the query - this will create an InMemory table
            let mut modified_statement = CreateTableStatement {
                name: ObjectName(vec![new_table_name.clone()]),
                transient: false,
                ..create_table_statement
            };

            // Replace qualify with nested select
            if let Some(ref mut query) = modified_statement.query {
                self.update_qualify_in_query(query);
            }
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

            let fields_with_ids = StructType::try_from(&new_fields_with_ids(
                plan.schema().as_arrow().fields(),
                &mut 0,
            ))
            .map_err(|err| DataFusionError::External(Box::new(err)))
            .context(super::error::DataFusionSnafu)?;
            let schema = Schema::builder()
                .with_schema_id(0)
                .with_identifier_field_ids(vec![])
                .with_fields(fields_with_ids)
                .build()
                .map_err(|err| DataFusionError::External(Box::new(err)))
                .context(super::error::DataFusionSnafu)?;

            // Check if it already exists, if it is - drop it
            // For now we behave as CREATE OR REPLACE
            // TODO support CREATE without REPLACE
            let catalog = self.ctx.catalog(warehouse_name).ok_or(
                ih_error::IcehutSQLError::WarehouseNotFound {
                    name: warehouse_name.to_string(),
                },
            )?;
            let iceberg_catalog = catalog.as_any().downcast_ref::<IcebergCatalog>().ok_or(
                ih_error::IcehutSQLError::IcebergCatalogNotFound {
                    warehouse_name: warehouse_name.to_string(),
                },
            )?;
            let rest_catalog = iceberg_catalog.catalog();
            let new_table_name = self.ident_normalizer.normalize(new_table_name.clone());
            let new_table_ident = Identifier::new(
                &new_table_db
                    .iter()
                    .map(|v| self.ident_normalizer.normalize(v.clone()))
                    .collect::<Vec<String>>(),
                &new_table_name.clone(),
            );
            if matches!(
                rest_catalog.tabular_exists(&new_table_ident).await,
                Ok(true)
            ) {
                rest_catalog
                    .drop_table(&new_table_ident)
                    .await
                    .context(ih_error::IcebergSnafu)?;
            };

            // Create new table
            rest_catalog
                .create_table(
                    new_table_ident.clone(),
                    CreateTableCatalog {
                        name: new_table_name.clone(),
                        location,
                        schema,
                        partition_spec: None,
                        write_order: None,
                        stage_create: None,
                        properties: None,
                    },
                )
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))
                .context(super::error::DataFusionSnafu)?;

            // Insert data to new table
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

            created_entity_response().context(ih_error::ArrowSnafu)
        } else {
            Err(super::error::IcehutSQLError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only CREATE TABLE statements are supported".to_string(),
                ),
            })
        }
    }

    /// This is experimental CREATE STAGE support
    /// Current limitations    
    /// TODO
    /// - Prepare object storage depending on the URL. Currently we support only s3 public buckets    ///   with public access with default eu-central-1 region
    /// - Parse credentials from specified config
    /// - We don't need to create table in case we have common shared session context.
    ///   CSV is registered as a table which can referenced from SQL statements executed against this context
    pub async fn create_stage_query(
        &self,
        statement: Statement,
        warehouse_name: &str,
    ) -> IcehutSQLResult<Vec<RecordBatch>> {
        if let Statement::CreateStage {
            name,
            stage_params,
            file_format,
            ..
        } = statement
        {
            let table_name = name
                .0
                .last()
                .ok_or_else(|| IcehutSQLError::InvalidIdentifier {
                    ident: name.to_string(),
                })?
                .clone();

            let skip_header = file_format.options.iter().any(|option| {
                option.option_name.eq_ignore_ascii_case("skip_header")
                    && option.value.eq_ignore_ascii_case("true")
            });

            let field_optionally_enclosed_by = file_format
                .options
                .iter()
                .find_map(|option| {
                    if option
                        .option_name
                        .eq_ignore_ascii_case("field_optionally_enclosed_by")
                    {
                        Some(option.value.as_bytes()[0])
                    } else {
                        None
                    }
                })
                .unwrap_or(b'"');

            let file_path = stage_params.url.unwrap_or_default();
            let stage_table_name = format!("stage_{table_name}");
            let url =
                Url::parse(file_path.as_str()).map_err(|_| IcehutSQLError::InvalidIdentifier {
                    ident: file_path.clone(),
                })?;
            let bucket = url.host_str().unwrap_or_default();
            // TODO Prepare object storage depending on the URL
            let s3 = AmazonS3Builder::from_env()
                // TODO Get region automatically
                .with_region("eu-central-1")
                .with_bucket_name(bucket)
                .build()
                .map_err(|_| IcehutSQLError::InvalidIdentifier {
                    ident: bucket.to_string(),
                })?;

            self.ctx.register_object_store(&url, Arc::new(s3));

            // Read CSV file to get default schema
            let csv_data = self
                .ctx
                .read_csv(
                    file_path.clone(),
                    CsvReadOptions::new()
                        .has_header(skip_header)
                        .quote(field_optionally_enclosed_by),
                )
                .await
                .context(ih_error::DataFusionSnafu)?;

            let fields = csv_data
                .schema()
                .iter()
                .map(|(_, field)| {
                    let data_type = if matches!(field.data_type(), DataType::Null) {
                        DataType::Utf8
                    } else {
                        field.data_type().clone()
                    };
                    Field::new(field.name(), data_type, field.is_nullable())
                })
                .collect::<Vec<_>>();

            // Register CSV file with filled missing datatype with default Utf8
            self.ctx
                .register_csv(
                    stage_table_name.clone(),
                    file_path,
                    CsvReadOptions::new()
                        .has_header(skip_header)
                        .quote(field_optionally_enclosed_by)
                        .schema(&ArrowSchema::new(fields)),
                )
                .await
                .context(ih_error::DataFusionSnafu)?;

            // Create stages database and create table with prepared schema
            // TODO Don't create table in case we have common ctx
            self.create_database(warehouse_name, ObjectName(vec![Ident::new("stages")]), true)
                .await?;
            let create_query = format!("CREATE TABLE {warehouse_name}.stages.{table_name} AS (SELECT * FROM {stage_table_name})");
            self.query(&create_query, warehouse_name, "").await
        } else {
            Err(IcehutSQLError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only CREATE STAGE statements are supported".to_string(),
                ),
            })
        }
    }

    pub async fn copy_into_snowflake_query(
        &self,
        statement: Statement,
        warehouse_name: &str,
    ) -> IcehutSQLResult<Vec<RecordBatch>> {
        if let Statement::CopyIntoSnowflake {
            into, from_stage, ..
        } = statement
        {
            // Insert data to table
            let from_query = from_stage.to_string().replace('@', "");
            let insert_query =
                format!("INSERT INTO {into} SELECT * FROM {warehouse_name}.stages.{from_query}");
            self.execute_with_custom_plan(&insert_query, warehouse_name)
                .await
        } else {
            Err(IcehutSQLError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only COPY INTO statements are supported".to_string(),
                ),
            })
        }
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn merge_query(
        &self,
        statement: Statement,
        warehouse_name: &str,
    ) -> IcehutSQLResult<Vec<RecordBatch>> {
        if let Statement::Merge {
            mut table,
            mut source,
            on,
            clauses,
            ..
        } = statement
        {
            self.update_tables_in_table_factor(&mut table, warehouse_name);
            self.update_tables_in_table_factor(&mut source, warehouse_name);

            let (target_table, target_alias) = Self::get_table_with_alias(table);
            let (_source_table, source_alias) = Self::get_table_with_alias(source.clone());

            let source_query = if let TableFactor::Derived {
                subquery,
                lateral,
                alias,
            } = source
            {
                source = TableFactor::Derived {
                    lateral,
                    subquery,
                    alias: None,
                };
                alias.map_or_else(|| source.to_string(), |alias| format!("{source} {alias}"))
            } else {
                source.to_string()
            };

            // Prepare WHERE clause to filter unmatched records
            let where_clause = self
                .get_expr_where_clause(*on.clone(), target_alias.as_str())
                .iter()
                .map(|v| format!("{v} IS NULL"))
                .collect::<Vec<_>>();
            let where_clause_str = if where_clause.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", where_clause.join(" AND "))
            };

            // Check NOT MATCHED for records to INSERT
            // Extract columns and values from clauses
            let mut columns = String::new();
            let mut values = String::new();
            for clause in clauses {
                if clause.clause_kind == MergeClauseKind::NotMatched {
                    if let MergeAction::Insert(insert) = clause.action {
                        columns = insert
                            .columns
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ");
                        if let MergeInsertKind::Values(values_insert) = insert.kind {
                            values = values_insert
                                .rows
                                .into_iter()
                                .flatten()
                                .collect::<Vec<_>>()
                                .iter()
                                .map(|v| format!("{source_alias}.{v}"))
                                .collect::<Vec<_>>()
                                .join(", ");
                        }
                    }
                }
            }
            let select_query =
                format!("SELECT {values} FROM {source_query} JOIN {target_table} {target_alias} ON {on}{where_clause_str}");

            // Construct the INSERT statement
            let insert_query = format!("INSERT INTO {target_table} ({columns}) {select_query}");
            self.execute_with_custom_plan(&insert_query, warehouse_name)
                .await
        } else {
            Err(super::error::IcehutSQLError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only MERGE statements are supported".to_string(),
                ),
            })
        }
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn drop_query(
        &self,
        query: &str,
        warehouse_name: &str,
    ) -> IcehutSQLResult<Vec<RecordBatch>> {
        let plan = self.get_custom_logical_plan(query, warehouse_name).await?;
        let transformed = plan
            .transform(iceberg_transform)
            .data()
            .context(ih_error::DataFusionSnafu)?;
        let res = self
            .ctx
            .execute_logical_plan(transformed)
            .await
            .context(ih_error::DataFusionSnafu)?
            .collect()
            .await
            .context(ih_error::DataFusionSnafu)?;
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn create_schema(
        &self,
        warehouse_name: &str,
        name: SchemaName,
        if_not_exists: bool,
    ) -> IcehutSQLResult<Vec<RecordBatch>> {
        match name {
            SchemaName::Simple(schema_name) => {
                self.create_database(warehouse_name, schema_name.clone(), if_not_exists)
                    .await?;
            }
            _ => {
                return Err(super::error::IcehutSQLError::DataFusion {
                    source: DataFusionError::NotImplemented(
                        "Only simple schema names are supported".to_string(),
                    ),
                });
            }
        }
        created_entity_response().context(super::error::ArrowSnafu)
    }

    pub async fn create_database(
        &self,
        warehouse_name: &str,
        name: ObjectName,
        _if_not_exists: bool,
    ) -> IcehutSQLResult<Vec<RecordBatch>> {
        // TODO: Abstract the Iceberg catalog
        let catalog = self.ctx.catalog(warehouse_name).ok_or(
            ih_error::IcehutSQLError::WarehouseNotFound {
                name: warehouse_name.to_string(),
            },
        )?;
        let iceberg_catalog = catalog.as_any().downcast_ref::<IcebergCatalog>().ok_or(
            ih_error::IcehutSQLError::IcebergCatalogNotFound {
                warehouse_name: warehouse_name.to_string(),
            },
        )?;
        let rest_catalog = iceberg_catalog.catalog();
        let namespace_vec: Vec<String> = name
            .0
            .iter()
            .map(|ident| self.ident_normalizer.normalize(ident.clone()))
            .collect();
        let single_layer_namespace = vec![namespace_vec.join(".")];

        let namespace =
            Namespace::try_new(&single_layer_namespace).context(ih_error::IcebergSpecSnafu)?;
        let exists = rest_catalog.load_namespace(&namespace).await.is_ok();
        if !exists {
            rest_catalog
                .create_namespace(&namespace, None)
                .await
                .context(ih_error::IcebergSnafu)?;
        }
        created_entity_response().context(super::error::ArrowSnafu)
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn get_custom_logical_plan(
        &self,
        query: &str,
        warehouse_name: &str,
    ) -> IcehutSQLResult<LogicalPlan> {
        let state = self.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        let mut statement = state
            .sql_to_statement(query, dialect)
            .context(super::error::DataFusionSnafu)?;
        //println!("raw query: {:?}", statement.to_string());
        statement = self.update_statement_references(statement, warehouse_name);
        //println!("modified query: {:?}", statement.to_string());

        if let DFStatement::Statement(s) = statement.clone() {
            let mut ctx_provider = SessionContextProvider {
                state: &state,
                tables: HashMap::new(),
            };

            let references = state
                .resolve_table_references(&statement)
                .context(super::error::DataFusionSnafu)?;
            //println!("References: {:?}", references);
            for reference in references {
                let resolved = state.resolve_table_ref(reference);
                if let Entry::Vacant(v) = ctx_provider.tables.entry(resolved.clone()) {
                    if let Ok(schema) = state.schema_for_ref(resolved.clone()) {
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
                            .ok_or(IcehutSQLError::TableProviderNotFound {
                                table_name: table.clone(),
                            })?;
                        let resolved = state.resolve_table_ref(TableReference::full(
                            catalog.to_string(),
                            schema.to_string(),
                            table,
                        ));
                        ctx_provider
                            .tables
                            .insert(resolved, provider_as_source(table_source));
                    }
                }
            }

            let planner =
                ExtendedSqlToRel::new(&ctx_provider, self.ctx.state().get_parser_options());
            planner
                .sql_statement_to_plan(*s)
                .context(super::error::DataFusionSnafu)
        } else {
            Err(super::error::IcehutSQLError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only SQL statements are supported".to_string(),
                ),
            })
        }
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn execute_with_custom_plan(
        &self,
        query: &str,
        warehouse_name: &str,
    ) -> IcehutSQLResult<Vec<RecordBatch>> {
        let plan = self.get_custom_logical_plan(query, warehouse_name).await?;
        self.ctx
            .execute_logical_plan(plan)
            .await
            .context(super::error::DataFusionSnafu)?
            .collect()
            .await
            .context(super::error::DataFusionSnafu)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn update_qualify_in_query(&self, query: &mut Query) {
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                self.update_qualify_in_query(&mut cte.query);
            }
        }

        match query.body.as_mut() {
            sqlparser::ast::SetExpr::Select(select) => {
                if let Some(Expr::BinaryOp { left, op, right }) = select.qualify.as_ref() {
                    if matches!(
                        op,
                        BinaryOperator::Eq | BinaryOperator::Lt | BinaryOperator::LtEq
                    ) {
                        let mut inner_select = select.clone();
                        inner_select.qualify = None;
                        inner_select.projection.push(SelectItem::ExprWithAlias {
                            expr: *(left.clone()),
                            alias: Ident {
                                value: "qualify_alias".to_string(),
                                quote_style: None,
                                span: Span::empty(),
                            },
                        });
                        let subquery = Query {
                            with: None,
                            body: Box::new(sqlparser::ast::SetExpr::Select(inner_select)),
                            order_by: None,
                            limit: None,
                            limit_by: vec![],
                            offset: None,
                            fetch: None,
                            locks: vec![],
                            for_clause: None,
                            settings: None,
                            format_clause: None,
                        };
                        let outer_select = Select {
                            select_token: AttachedToken::empty(),
                            distinct: None,
                            top: None,
                            top_before_distinct: false,
                            projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                                value: "*".to_string(),
                                quote_style: None,
                                span: Span::empty(),
                            }))],
                            into: None,
                            from: vec![TableWithJoins {
                                relation: TableFactor::Derived {
                                    lateral: false,
                                    subquery: Box::new(subquery),
                                    alias: None,
                                },
                                joins: vec![],
                            }],
                            lateral_views: vec![],
                            prewhere: None,
                            selection: Some(Expr::BinaryOp {
                                left: Box::new(Expr::Identifier(Ident {
                                    value: "qualify_alias".to_string(),
                                    quote_style: None,
                                    span: Span::empty(),
                                })),
                                op: op.clone(),
                                right: Box::new(*right.clone()),
                            }),
                            group_by: GroupByExpr::Expressions(vec![], vec![]),
                            cluster_by: vec![],
                            distribute_by: vec![],
                            sort_by: vec![],
                            having: None,
                            named_window: vec![],
                            qualify: None,
                            window_before_qualify: false,
                            value_table_mode: None,
                            connect_by: None,
                        };

                        *query.body = sqlparser::ast::SetExpr::Select(Box::new(outer_select));
                    }
                }
            }
            sqlparser::ast::SetExpr::Query(q) => {
                self.update_qualify_in_query(q);
            }
            _ => {}
        }
    }

    fn update_table_result_scan_in_query(query: &mut Query) {
        // TODO: Add logic to get result_scan from the historical results
        if let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() {
            // Remove is_iceberg field since it is not supported by information_schema.tables
            select.projection.retain(|field| {
                if let SelectItem::UnnamedExpr(Expr::Identifier(ident)) = field {
                    ident.value.to_lowercase() != "is_iceberg"
                } else {
                    true
                }
            });

            // Replace result_scan with the select from information_schema.tables
            for table_with_joins in &mut select.from {
                if let TableFactor::TableFunction {
                    expr: Expr::Function(f),
                    alias,
                } = &mut table_with_joins.relation
                {
                    if f.name.to_string().to_lowercase() == "result_scan" {
                        let columns = [
                            "table_catalog as 'database_name'",
                            "table_schema as 'schema_name'",
                            "table_name as 'name'",
                            "case when table_type='BASE TABLE' then 'TABLE' else table_type end as 'kind'",
                            "null as 'comment'",
                            "case when table_type='BASE TABLE' then 'Y' else 'N' end as is_iceberg",
                            "'N' as 'is_dynamic'",
                        ].join(", ");
                        let information_schema_query =
                            format!("SELECT {columns} FROM information_schema.tables");

                        match DFParser::parse_sql(information_schema_query.as_str()) {
                            Ok(mut statements) => {
                                if let Some(DFStatement::Statement(s)) = statements.pop_front() {
                                    if let Statement::Query(subquery) = *s {
                                        select.from = vec![TableWithJoins {
                                            relation: TableFactor::Derived {
                                                lateral: false,
                                                alias: alias.clone(),
                                                subquery,
                                            },
                                            joins: table_with_joins.joins.clone(),
                                        }];
                                        break;
                                    }
                                }
                            }
                            Err(_) => return,
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn get_expr_where_clause(&self, expr: Expr, target_alias: &str) -> Vec<String> {
        match expr {
            Expr::CompoundIdentifier(ident) => {
                if ident.len() > 1 && ident[0].value == target_alias {
                    let ident_str = ident
                        .iter()
                        .map(|v| v.value.clone())
                        .collect::<Vec<String>>()
                        .join(".");
                    return vec![ident_str];
                }
                vec![]
            }
            Expr::BinaryOp { left, right, .. } => {
                let mut left_expr = self.get_expr_where_clause(*left, target_alias);
                let right_expr = self.get_expr_where_clause(*right, target_alias);
                left_expr.extend(right_expr);
                left_expr
            }
            _ => vec![],
        }
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn get_table_path(&self, statement: &DFStatement) -> TablePath {
        let empty = String::new;
        let table_path = |arr: &Vec<Ident>| -> TablePath {
            match arr.len() {
                1 => TablePath {
                    db: empty(),
                    schema: empty(),
                    table: arr[0].value.clone(),
                },
                2 => TablePath {
                    db: empty(),
                    schema: arr[0].value.clone(),
                    table: arr[1].value.clone(),
                },
                3 => TablePath {
                    db: arr[0].value.clone(),
                    schema: arr[1].value.clone(),
                    table: arr[2].value.clone(),
                },
                _ => TablePath {
                    db: empty(),
                    schema: empty(),
                    table: empty(),
                },
            }
        };

        match statement.clone() {
            DFStatement::CreateExternalTable(create_external) => {
                table_path(&create_external.name.0)
            }
            DFStatement::Statement(s) => match *s {
                Statement::AlterTable { name, .. } => table_path(&name.0),
                Statement::Insert(insert) => table_path(&insert.table_name.0),
                Statement::Drop { names, .. } => table_path(&names[0].0),
                Statement::Query(query) => match *query.body {
                    sqlparser::ast::SetExpr::Select(select) => {
                        if select.from.is_empty() {
                            table_path(&vec![])
                        } else {
                            match &select.from[0].relation {
                                TableFactor::Table { name, .. } => table_path(&name.0),
                                _ => table_path(&vec![]),
                            }
                        }
                    }
                    _ => table_path(&vec![]),
                },
                Statement::CreateTable(create_table) => table_path(&create_table.name.0),
                Statement::Update { table, .. } => match table.relation {
                    TableFactor::Table { name, .. } => table_path(&name.0),
                    _ => table_path(&vec![]),
                },
                Statement::CreateSchema {
                    schema_name: SchemaName::Simple(name),
                    ..
                } => {
                    if name.0.len() == 2 {
                        TablePath {
                            db: name.0[0].value.clone(),
                            schema: name.0[1].value.clone(),
                            table: empty(),
                        }
                    } else {
                        TablePath {
                            db: empty(),
                            schema: name.0[1].value.clone(),
                            table: empty(),
                        }
                    }
                }
                _ => table_path(&vec![]),
            },
            _ => table_path(&vec![]),
        }
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
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
                Statement::AlterTable {
                    name,
                    if_exists,
                    only,
                    operations,
                    location,
                    on_cluster,
                } => {
                    let name = self.compress_database_name(name.0, warehouse_name);
                    let modified_statement = Statement::AlterTable {
                        name: ObjectName(name),
                        if_exists,
                        only,
                        operations,
                        location,
                        on_cluster,
                    };
                    DFStatement::Statement(Box::new(modified_statement))
                }
                Statement::Insert(insert_statement) => {
                    let table_name =
                        self.compress_database_name(insert_statement.table_name.0, warehouse_name);

                    let source = insert_statement.source.map_or_else(
                        || None,
                        |mut query| {
                            self.update_tables_in_query(query.as_mut(), warehouse_name);
                            Some(Box::new(AstQuery { ..*query }))
                        },
                    );

                    let modified_statement = Insert {
                        table_name: ObjectName(table_name),
                        source,
                        ..insert_statement
                    };
                    DFStatement::Statement(Box::new(Statement::Insert(modified_statement)))
                }
                Statement::Drop {
                    object_type,
                    if_exists,
                    mut names,
                    cascade,
                    restrict,
                    purge,
                    temporary,
                } => {
                    match object_type {
                        ObjectType::Database | ObjectType::Schema => {
                            let mut table_name = names[0].0.clone();
                            if !warehouse_name.is_empty()
                                && !table_name.starts_with(&[Ident::new(warehouse_name)])
                            {
                                table_name.insert(0, Ident::new(warehouse_name));
                            }
                            names = vec![ObjectName(table_name)];
                        }
                        _ => {}
                    }

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
                Statement::Update {
                    mut table,
                    assignments,
                    mut from,
                    selection,
                    returning,
                    or,
                } => {
                    self.update_tables_in_table_with_joins(&mut table, warehouse_name);
                    if let Some(from) = from.as_mut() {
                        self.update_tables_in_table_with_joins(from, warehouse_name);
                    }
                    let modified_statement = Statement::Update {
                        table,
                        assignments,
                        from,
                        selection,
                        returning,
                        or,
                    };
                    DFStatement::Statement(Box::new(modified_statement))
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

    #[allow(clippy::only_used_in_recursion)]
    fn get_table_with_alias(factor: TableFactor) -> (ObjectName, String) {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let target_alias = alias.map_or_else(String::new, |alias| alias.to_string());
                (name, target_alias)
            }
            // Return only alias for derived tables
            TableFactor::Derived { alias, .. } => {
                let target_alias = alias.map_or_else(String::new, |alias| alias.to_string());
                (ObjectName(vec![]), target_alias)
            }
            _ => (ObjectName(vec![]), String::new()),
        }
    }

    fn update_tables_in_query(&self, query: &mut Query, warehouse_name: &str) {
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                self.update_tables_in_query(&mut cte.query, warehouse_name);
            }
        }

        match query.body.as_mut() {
            sqlparser::ast::SetExpr::Select(select) => {
                for table_with_joins in &mut select.from {
                    self.update_tables_in_table_with_joins(table_with_joins, warehouse_name);
                }

                if let Some(expr) = &mut select.selection {
                    self.update_tables_in_expr(expr, warehouse_name);
                }
            }
            sqlparser::ast::SetExpr::Query(q) => {
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

pub fn created_entity_response() -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    Ok(vec![RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![0]))],
    )?])
}
