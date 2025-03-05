use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::datasource::default_table_source::provider_as_source;
use datafusion::execution::session_state::SessionContextProvider;
use datafusion::logical_expr::sqlparser::ast::Insert;
use datafusion::logical_expr::LogicalPlan;
use datafusion::parquet::schema;
use datafusion::prelude::CsvReadOptions;
use datafusion::sql::parser::{CreateExternalTable, DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    CreateTable as CreateTableStatement, Expr, Ident, ObjectName, Query, SchemaName, Statement,
    TableFactor, TableWithJoins,
};
use datafusion_common::{plan_err, DataFusionError, Result as DFResult, TableReference};
use datafusion_functions_json::register_all;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use datafusion_iceberg::planner::iceberg_transform;
use geodatafusion::udf::native::register_native;
use iceberg_rust::catalog::create::CreateTable as CreateTableCatalog;
use iceberg_rust::spec::arrow::schema::new_fields_with_ids;
use iceberg_rust::spec::identifier::Identifier;
use iceberg_rust::spec::namespace::Namespace;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::StructType;
use iceberg_rust_spec::types::StructField;
use icebucket_metastore::{IceBucketTableCreateRequest, IceBucketTableIdent, Metastore};
use object_store::aws::AmazonS3Builder;
use serde::Deserialize;
use snafu::ResultExt;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    visit_expressions_mut, BinaryOperator, ColumnDef, GroupByExpr, MergeAction, MergeClauseKind, MergeInsertKind, ObjectType, Query as AstQuery, Select, SelectItem, Use
};
use sqlparser::tokenizer::Span;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use url::Url;

use super::datafusion::functions::visit_functions_expressions;
use super::datafusion::planner::ExtendedSqlToRel;
use super::error::{self as ex_error, ExecutionError, ExecutionResult};

use super::session::IceBucketUserSession;

#[derive(Default, Debug, Deserialize)]
pub struct IceBucketQueryContext {
    pub database: Option<String>,
    pub schema: Option<String>,
}

pub struct IceBucketQuery {
    pub metastore: Arc<dyn Metastore>,
    pub query: String,
    pub session: Arc<IceBucketUserSession>,
    pub query_context: IceBucketQueryContext
}

impl IceBucketQuery {
    pub fn new(
            session: Arc<IceBucketUserSession>,
            metastore: Arc<dyn Metastore>, 
            query: String, 
            query_context: IceBucketQueryContext
            ) -> Self {
        Self {
            metastore,
            query,
            session,
            query_context
        }
    }

    pub fn parse_query(&self, query: &str) -> Result<DFStatement, DataFusionError> {
        let state = self.session.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        state.sql_to_statement(query, dialect)
    }

    #[allow(clippy::unwrap_used)]
    #[tracing::instrument(level = "trace", ret)]
    pub fn postprocess_query_statement(statement: &mut DFStatement) {
        if let DFStatement::Statement(value) = statement {
            visit_expressions_mut(&mut *value, |expr| {
                if let Expr::Function(ref mut func) = expr {
                    visit_functions_expressions(func);
                }
                ControlFlow::<()>::Continue(())
            });
        }
    }

    fn current_database(&self) -> Option<String> {
        self.query_context.database.clone().or_else(|| self.session.get_session_variable("database"))
    }

    fn current_schema(&self) -> Option<String> {
        self.query_context.schema.clone().or_else(|| self.session.get_session_variable("schema"))
    }

    #[tracing::instrument(level = "debug", skip(self), err, ret(level = tracing::Level::TRACE))]
    pub async fn execute(
        &self,
    ) -> ExecutionResult<Vec<RecordBatch>> {
        // Update query to use custom JSON functions
        let query = self.preprocess_query(&self.query);
        let mut statement = self
            .parse_query(query.as_str())
            .context(super::error::DataFusionSnafu)?;
        Self::postprocess_query_statement(&mut statement);

        // statement = self.update_statement_references(statement, warehouse_name);
        // query = statement.to_string();

        // TODO: Code should be organized in a better way
        // 1. Single place to parse SQL strings into AST
        // 2. Single place to update AST
        // 3. Single place to construct Logical plan from this AST
        // 4. Single place to rewrite-optimize-adjust logical plan
        // etc
        if let DFStatement::Statement(s) = statement {
            match *s {
                Statement::AlterSession {
                    set,
                    session_params,
                } => {
                    let params = session_params
                        .options
                        .into_iter()
                        .map(|v| (v.option_name, v.value))
                        .collect();

                    self.session.set_session_variable(set, params)?;
                    return Ok(vec![]);
                }
                Statement::Use(entity) => {
                    let (variable, value) = match entity {
                        Use::Catalog(n) => ("catalog", n.to_string()),
                        Use::Schema(n) => ("schema", n.to_string()),
                        Use::Database(n) => ("database", n.to_string()),
                        Use::Warehouse(n) => ("warehouse", n.to_string()),
                        Use::Role(n) => ("role", n.to_string()),
                        Use::Object(n) => ("object", n.to_string()),
                        Use::SecondaryRoles(sr) => ("secondary_roles", sr.to_string()),
                        Use::Default => ("", String::new()),
                    };
                    if variable.is_empty() | value.is_empty() {
                        return Err(ExecutionError::DataFusion {
                            source: DataFusionError::NotImplemented(
                                "Only USE with variables are supported".to_string(),
                            ),
                        });
                    }
                    let params = HashMap::from([(variable.to_string(), value)]);
                    self.session.set_session_variable(true, params);
                    return Ok(vec![]);
                }
                Statement::SetVariable {
                    variables, value, ..
                } => {
                    let keys = variables.iter().map(ToString::to_string);
                    let values: ExecutionResult<Vec<_>> = value
                        .iter()
                        .map(|v| match v {
                            Expr::Identifier(_) | Expr::Value(_) => Ok(v.to_string()),
                            _ => Err(ExecutionError::DataFusion {
                                source: DataFusionError::NotImplemented(
                                    "Only primitive statements are supported".to_string(),
                                ),
                            }),
                        })
                        .collect();
                    let values = values?;
                    let params = keys.into_iter().zip(values.into_iter()).collect();
                    self.session.set_session_variable(true, params);
                    return Ok(vec![]);
                }
                Statement::CreateTable { .. } => {
                    return Box::pin(self.create_table_query(*s)).await;
                }
                Statement::CreateDatabase {
                    db_name,
                    if_not_exists,
                    ..
                } => {
                    return self
                        .create_database(db_name, if_not_exists)
                        .await;
                }
                Statement::CreateSchema {
                    schema_name,
                    if_not_exists,
                } => {
                    return self
                        .create_schema(schema_name, if_not_exists)
                        .await;
                }
                Statement::CreateStage { .. } => {
                    // We support only CSV uploads for now
                    return Box::pin(self.create_stage_query(*s)).await;
                }
                Statement::CopyIntoSnowflake { .. } => {
                    return Box::pin(self.copy_into_snowflake_query(*s)).await;
                }
                Statement::AlterTable { .. }
                | Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Insert { .. }
                | Statement::ShowSchemas { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowObjects { .. }
                | Statement::Update { .. } => {
                    return Box::pin(self.execute_with_custom_plan(&query)).await;
                }
                Statement::Query(mut subquery) => {
                    self.update_qualify_in_query(subquery.as_mut());
                    Self::update_table_result_scan_in_query(subquery.as_mut());
                    return Box::pin(
                        self.execute_with_custom_plan(&subquery.to_string()),
                    )
                    .await;
                }
                Statement::Drop { .. } => {
                    return Box::pin(self.drop_query(&query)).await;
                }
                Statement::Merge { .. } => {
                    return Box::pin(self.merge_query(*s)).await;
                }
                _ => {}
            }
        }
        self.session.ctx
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

        let mut query = re
            .replace_all(query, "json_get(json_get($1, $2), '$3')")
            .to_string();
        query = date_add.replace_all(&query, "$1$2('$3',").to_string();
        let alter_iceberg_table = regex::Regex::new(r"alter\s+iceberg\s+table").unwrap();
        query = alter_iceberg_table
            .replace_all(&query, "alter table")
            .to_string();
        // TODO remove this check after release of https://github.com/Embucket/datafusion-sqlparser-rs/pull/8
        if query.to_lowercase().contains("alter session") {
            query = query.replace(';', "");
        }
        query
            .replace("skip_header=1", "skip_header=TRUE")
            .replace("FROM @~/", "FROM ")
    }

    #[allow(clippy::redundant_else, clippy::too_many_lines)]
    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn create_table_query(
        &self,
        statement: Statement,
    ) -> ExecutionResult<Vec<RecordBatch>> {
        if let Statement::CreateTable(create_table_statement) = statement {
            let mut new_table_ident = self.resolve_table_ident(create_table_statement.name.0)?;

            let table_location = create_table_statement.location.clone()
                .or_else(|| create_table_statement.base_location.clone());

            let table_name = new_table_ident.last().unwrap().clone();
            // Replace the name of table that needs creation (for ex. "warehouse"."database"."table" -> "table")
            // And run the query - this will create an InMemory table
            let mut modified_statement = CreateTableStatement {
                name: ObjectName(vec![table_name.clone()]),
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
                .get_custom_logical_plan(&updated_query)
                .await?;
            self.session.ctx
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
            let ib_table_ident = IceBucketTableIdent {
                database: new_table_ident[0].to_string(),
                schema: new_table_ident[1].to_string(),
                table: new_table_ident[2].to_string()
            };

            let table_exists = self.metastore.table_exists(&ib_table_ident).await
                .context(ex_error::MetastoreSnafu)?;

            if table_exists && create_table_statement.or_replace {
                self.metastore
                        .delete_table(&ib_table_ident, true)
                        .await
                        .context(ex_error::MetastoreSnafu)?;
            } else if table_exists && create_table_statement.if_not_exists {
                return Err(ExecutionError::ObjectAlreadyExists { type_name: "table".to_string(), name: ib_table_ident.to_string() });
            }

            // TODO: Gather volume properties from the .options field
            let table_create_request = IceBucketTableCreateRequest {
                ident: ib_table_ident.clone(),
                schema,
                location: table_location,
                partition_spec: None,
                sort_order: None,
                stage_create: None,
                volume_ident: None,
                is_temporary: Some(create_table_statement.temporary),
                format: None,
                properties: None,
            };

            self.metastore.create_table(ib_table_ident.clone(), table_create_request).await
                .context(ex_error::MetastoreSnafu)?;

            // Insert data to new table
            // TODO: What is the point of this?
            let insert_query =
                format!("INSERT INTO {} SELECT * FROM {}", ib_table_ident.to_string(), table_name);
            self.execute_with_custom_plan(&insert_query)
                .await?;

            // Drop InMemory table
            let drop_query = format!("DROP TABLE {}", table_name);
            self.session.ctx
                .sql(&drop_query)
                .await
                .context(super::error::DataFusionSnafu)?
                .collect()
                .await
                .context(super::error::DataFusionSnafu)?;

            created_entity_response()
        } else {
            Err(ExecutionError::DataFusion {
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
    ) -> ExecutionResult<Vec<RecordBatch>> {
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
                .ok_or_else(|| ExecutionError::InvalidIdentifier {
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
            let url = Url::parse(file_path.as_str()).map_err(|_| {
                ExecutionError::InvalidIdentifier {
                    ident: file_path.clone(),
                }
            })?;
            let bucket = url.host_str().unwrap_or_default();
            // TODO Prepare object storage depending on the URL
            let s3 = AmazonS3Builder::from_env()
                // TODO Get region automatically
                .with_region("eu-central-1")
                .with_bucket_name(bucket)
                .build()
                .map_err(|_| ExecutionError::InvalidIdentifier {
                    ident: bucket.to_string(),
                })?;

            self.session.ctx.register_object_store(&url, Arc::new(s3));

            // Read CSV file to get default schema
            let csv_data = self
                .session
                .ctx
                .read_csv(
                    file_path.clone(),
                    CsvReadOptions::new()
                        .has_header(skip_header)
                        .quote(field_optionally_enclosed_by),
                )
                .await
                .context(ex_error::DataFusionSnafu)?;

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
            self.session
                .ctx
                .register_csv(
                    table_name.value.clone(),
                    file_path,
                    CsvReadOptions::new()
                        .has_header(skip_header)
                        .quote(field_optionally_enclosed_by)
                        .schema(&ArrowSchema::new(fields)),
                )
                .await
                .context(ex_error::DataFusionSnafu)?;
            Ok(vec![])
        } else {
            Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only CREATE STAGE statements are supported".to_string(),
                ),
            })
        }
    }

    pub async fn copy_into_snowflake_query(
        &self,
        statement: Statement,
    ) -> ExecutionResult<Vec<RecordBatch>> {
        if let Statement::CopyIntoSnowflake {
            into, from_stage, ..
        } = statement
        {
            // Insert data to table
            let stage_name = from_stage.to_string().replace('@', "");
            let insert_query = format!("INSERT INTO {into} SELECT * FROM {stage_name}");
            self.execute_with_custom_plan(&insert_query, warehouse_name)
                .await
        } else {
            Err(ExecutionError::DataFusion {
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
    ) -> ExecutionResult<Vec<RecordBatch>> {
        if let Statement::Merge {
            table,
            mut source,
            on,
            clauses,
            ..
        } = statement
        {
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
            Err(ExecutionError::DataFusion {
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
    ) -> ExecutionResult<Vec<RecordBatch>> {
        let plan = self.get_custom_logical_plan(
            query, 
            &self.current_database(),
            &self.current_schema()
        ).await?;
        let transformed = plan
            .transform(iceberg_transform)
            .data()
            .context(ex_error::DataFusionSnafu)?;
        let res = self
            .session
            .ctx
            .execute_logical_plan(transformed)
            .await
            .context(ex_error::DataFusionSnafu)?
            .collect()
            .await
            .context(ex_error::DataFusionSnafu)?;
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn create_schema(
        &self,
        name: SchemaName,
        if_not_exists: bool,
    ) -> ExecutionResult<Vec<RecordBatch>> {
        match name {
            SchemaName::Simple(schema_name) => {
                self.create_database(warehouse_name, schema_name.clone(), if_not_exists)
                    .await?;
            }
            _ => {
                return Err(ExecutionError::DataFusion {
                    source: DataFusionError::NotImplemented(
                        "Only simple schema names are supported".to_string(),
                    ),
                });
            }
        }
        created_entity_response()
    }

    pub async fn create_database(
        &self,
        name: ObjectName,
        _if_not_exists: bool,
    ) -> ExecutionResult<Vec<RecordBatch>> {
        // Parse name into catalog (warehouse) name and schema name
        let current_database = self.current_database();
        let current_schema = self.current_schema();
        let object_name = self.compress_database_name(table_name, &current_database, &current_schema)?;

        let (warehouse_name, schema_name) = match name.0.len() {
            2 => (
                self.session.ident_normalizer.normalize(name.0[0].clone()),
                self.session.ident_normalizer.normalize(name.0[1].clone()),
            ),
            _ => {
                return Err(ExecutionError::DataFusion {
                    source: DataFusionError::NotImplemented(
                        "Only two-part names are supported".to_string(),
                    ),
                });
            }
        };

        // TODO: Abstract the Iceberg catalog
        let catalog = self.session.ctx.catalog(&warehouse_name).ok_or(
            ExecutionError::WarehouseNotFound {
                name: warehouse_name.to_string(),
            },
        )?;
        let iceberg_catalog = catalog.as_any().downcast_ref::<IcebergCatalog>().ok_or(
            ExecutionError::IcebergCatalogNotFound {
                warehouse_name: warehouse_name.to_string(),
            },
        )?;
        let rest_catalog = iceberg_catalog.catalog();
        let single_layer_namespace = vec![schema_name];

        let namespace =
            Namespace::try_new(&single_layer_namespace).context(ex_error::IcebergSpecSnafu)?;
        let exists = rest_catalog.load_namespace(&namespace).await.is_ok();
        if !exists {
            rest_catalog
                .create_namespace(&namespace, None)
                .await
                .context(ex_error::IcebergSnafu)?;
        }
        created_entity_response()
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn get_custom_logical_plan(
        &self,
        query: &str,
    ) -> ExecutionResult<LogicalPlan> {
        let state = self.session.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();

        // We turn a query to SQL only to turn it back into a statement
        // TODO: revisit this pattern
        let mut statement = state
            .sql_to_statement(query, dialect)
            .context(super::error::DataFusionSnafu)?;
        statement = self.update_statement_references(statement);

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
            for catalog in self.session.ctx.state().catalog_list().catalog_names() {
                let provider = self.session.ctx.state().catalog_list().catalog(&catalog).unwrap();
                for schema in provider.schema_names() {
                    for table in provider.schema(&schema).unwrap().table_names() {
                        let table_source = provider
                            .schema(&schema)
                            .unwrap()
                            .table(&table)
                            .await
                            .context(super::error::DataFusionSnafu)?
                            .ok_or(ExecutionError::TableProviderNotFound {
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
                ExtendedSqlToRel::new(&ctx_provider, self.session.ctx.state().get_parser_options());
            planner
                .sql_statement_to_plan(*s)
                .context(super::error::DataFusionSnafu)
        } else {
            Err(ExecutionError::DataFusion {
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
    ) -> ExecutionResult<Vec<RecordBatch>> {
        let plan = self.get_custom_logical_plan(query).await?;
        self.session
            .ctx
            .execute_logical_plan(plan)
            .await
            .context(super::error::DataFusionSnafu)?
            .collect()
            .await
            .context(super::error::DataFusionSnafu)
    }

    fn sql_schema_to_iceberg_schema(&self, start_index: u32, col_def: Vec<ColumnDef>) -> (u32, Vec<StructField>) {
        let mut index = start_index;
        let mut fields = vec![];
        for column in col_def {
            let field_type = match column.data_type {
                sqlparser::ast::DataType::Character(character_length) => todo!(),
                sqlparser::ast::DataType::Char(character_length) => todo!(),
                sqlparser::ast::DataType::CharacterVarying(character_length) => todo!(),
                sqlparser::ast::DataType::CharVarying(character_length) => todo!(),
                sqlparser::ast::DataType::Varchar(character_length) => todo!(),
                sqlparser::ast::DataType::Nvarchar(character_length) => todo!(),
                sqlparser::ast::DataType::Uuid => todo!(),
                sqlparser::ast::DataType::CharacterLargeObject(_) => todo!(),
                sqlparser::ast::DataType::CharLargeObject(_) => todo!(),
                sqlparser::ast::DataType::Clob(_) => todo!(),
                sqlparser::ast::DataType::Binary(_) => todo!(),
                sqlparser::ast::DataType::Varbinary(_) => todo!(),
                sqlparser::ast::DataType::Blob(_) => todo!(),
                sqlparser::ast::DataType::TinyBlob => todo!(),
                sqlparser::ast::DataType::MediumBlob => todo!(),
                sqlparser::ast::DataType::LongBlob => todo!(),
                sqlparser::ast::DataType::Bytes(_) => todo!(),
                sqlparser::ast::DataType::Numeric(exact_number_info) => todo!(),
                sqlparser::ast::DataType::Decimal(exact_number_info) => todo!(),
                sqlparser::ast::DataType::BigNumeric(exact_number_info) => todo!(),
                sqlparser::ast::DataType::BigDecimal(exact_number_info) => todo!(),
                sqlparser::ast::DataType::Dec(exact_number_info) => todo!(),
                sqlparser::ast::DataType::Float(_) => todo!(),
                sqlparser::ast::DataType::TinyInt(_) => todo!(),
                sqlparser::ast::DataType::UnsignedTinyInt(_) => todo!(),
                sqlparser::ast::DataType::Int2(_) => todo!(),
                sqlparser::ast::DataType::UnsignedInt2(_) => todo!(),
                sqlparser::ast::DataType::SmallInt(_) => todo!(),
                sqlparser::ast::DataType::UnsignedSmallInt(_) => todo!(),
                sqlparser::ast::DataType::MediumInt(_) => todo!(),
                sqlparser::ast::DataType::UnsignedMediumInt(_) => todo!(),
                sqlparser::ast::DataType::Int(_) => todo!(),
                sqlparser::ast::DataType::Int4(_) => todo!(),
                sqlparser::ast::DataType::Int8(_) => todo!(),
                sqlparser::ast::DataType::Int16 => todo!(),
                sqlparser::ast::DataType::Int32 => todo!(),
                sqlparser::ast::DataType::Int64 => todo!(),
                sqlparser::ast::DataType::Int128 => todo!(),
                sqlparser::ast::DataType::Int256 => todo!(),
                sqlparser::ast::DataType::Integer(_) => todo!(),
                sqlparser::ast::DataType::UnsignedInt(_) => todo!(),
                sqlparser::ast::DataType::UnsignedInt4(_) => todo!(),
                sqlparser::ast::DataType::UnsignedInteger(_) => todo!(),
                sqlparser::ast::DataType::UInt8 => todo!(),
                sqlparser::ast::DataType::UInt16 => todo!(),
                sqlparser::ast::DataType::UInt32 => todo!(),
                sqlparser::ast::DataType::UInt64 => todo!(),
                sqlparser::ast::DataType::UInt128 => todo!(),
                sqlparser::ast::DataType::UInt256 => todo!(),
                sqlparser::ast::DataType::BigInt(_) => todo!(),
                sqlparser::ast::DataType::UnsignedBigInt(_) => todo!(),
                sqlparser::ast::DataType::UnsignedInt8(_) => todo!(),
                sqlparser::ast::DataType::Float4 => todo!(),
                sqlparser::ast::DataType::Float32 => todo!(),
                sqlparser::ast::DataType::Float64 => todo!(),
                sqlparser::ast::DataType::Real => todo!(),
                sqlparser::ast::DataType::Float8 => todo!(),
                sqlparser::ast::DataType::Double => todo!(),
                sqlparser::ast::DataType::DoublePrecision => todo!(),
                sqlparser::ast::DataType::Bool => todo!(),
                sqlparser::ast::DataType::Boolean => todo!(),
                sqlparser::ast::DataType::Date => todo!(),
                sqlparser::ast::DataType::Date32 => todo!(),
                sqlparser::ast::DataType::Time(_, timezone_info) => todo!(),
                sqlparser::ast::DataType::Datetime(_) => todo!(),
                sqlparser::ast::DataType::Datetime64(_, _) => todo!(),
                sqlparser::ast::DataType::Timestamp(_, timezone_info) => todo!(),
                sqlparser::ast::DataType::Interval => todo!(),
                sqlparser::ast::DataType::JSON => todo!(),
                sqlparser::ast::DataType::JSONB => todo!(),
                sqlparser::ast::DataType::Regclass => todo!(),
                sqlparser::ast::DataType::Text => todo!(),
                sqlparser::ast::DataType::TinyText => todo!(),
                sqlparser::ast::DataType::MediumText => todo!(),
                sqlparser::ast::DataType::LongText => todo!(),
                sqlparser::ast::DataType::String(_) => todo!(),
                sqlparser::ast::DataType::FixedString(_) => todo!(),
                sqlparser::ast::DataType::Bytea => todo!(),
                sqlparser::ast::DataType::Bit(_) => todo!(),
                sqlparser::ast::DataType::BitVarying(_) => todo!(),
                sqlparser::ast::DataType::Custom(object_name, items) => todo!(),
                sqlparser::ast::DataType::Array(array_elem_type_def) => todo!(),
                sqlparser::ast::DataType::Map(data_type, data_type1) => todo!(),
                sqlparser::ast::DataType::Tuple(struct_fields) => todo!(),
                sqlparser::ast::DataType::Nested(column_defs) => todo!(),
                sqlparser::ast::DataType::Enum(enum_members, _) => todo!(),
                sqlparser::ast::DataType::Set(items) => todo!(),
                sqlparser::ast::DataType::Struct(struct_fields, struct_bracket_kind) => todo!(),
                sqlparser::ast::DataType::Union(union_fields) => todo!(),
                sqlparser::ast::DataType::Nullable(data_type) => todo!(),
                sqlparser::ast::DataType::LowCardinality(data_type) => todo!(),
                sqlparser::ast::DataType::Unspecified => todo!(),
                sqlparser::ast::DataType::Trigger => todo!(),
            }
            let field = StructField::new(
                index,
                column.name.value.as_str(),
                column.data_type.to_arrow(),
                column.is_nullable.unwrap_or(true),
            );
            index += 1;
            fields.push(field);
        }
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
    pub fn get_table_path(&self, statement: &DFStatement) -> Option<TableReference> {
        let empty = String::new;
        let references = self.session.ctx.state().resolve_table_references(statement).ok()?;

        match statement.clone() {
            DFStatement::Statement(s) => match *s {
                Statement::CopyIntoSnowflake { into, .. } => {
                    Some(TableReference::parse_str(&into.to_string()))
                }
                Statement::Drop { names, .. } => {
                    Some(TableReference::parse_str(&names[0].to_string()))
                }
                Statement::CreateSchema {
                    schema_name: SchemaName::Simple(name),
                    ..
                } => {
                    if name.0.len() == 2 {
                        Some(TableReference::full(
                            name.0[0].value.clone(),
                            name.0[1].value.clone(),
                            empty(),
                        ))
                    } else {
                        Some(TableReference::full(
                            empty(),
                            name.0[0].value.clone(),
                            empty(),
                        ))
                    }
                }
                _ => references.first().cloned(),
            },
            _ => references.first().cloned(),
        }
    }

    // TODO: Modify this function to modify the statement in-place to
    // avoid extra allocations
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn update_statement_references(
        &self,
        statement: DFStatement,
    ) -> ExecutionResult<DFStatement> {
        match statement.clone() {
            DFStatement::CreateExternalTable(create_external) => {
                let table_name = self.resolve_table_ident(create_external.name.0)?;
                let modified_statement = CreateExternalTable {
                    name: ObjectName(table_name),
                    ..create_external
                };
                Ok(DFStatement::CreateExternalTable(modified_statement))
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
                    let name = self.resolve_table_ident(table_ident)?;
                    let modified_statement = Statement::AlterTable {
                        name: ObjectName(name),
                        if_exists,
                        only,
                        operations,
                        location,
                        on_cluster,
                    };
                    Ok(DFStatement::Statement(Box::new(modified_statement)))
                }
                Statement::Insert(insert_statement) => {
                    let table_name = self.resolve_table_ident(table_ident)?;

                    let source = insert_statement.source.map_or_else(
                        || None,
                        |mut query| {
                            self.update_tables_in_query(query.as_mut(), database, schema)?;
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
                    for name in names.iter_mut() {
                        name = self.compress_database_name(name.0.clone(), database, schema)?;
                    }

                    let modified_statement = Statement::Drop {
                        object_type,
                        if_exists,
                        names,
                        cascade,
                        restrict,
                        purge,
                        temporary,
                    };
                    DFStatement::Statement(Box::new(modified_statement))
                }
                Statement::Query(mut query) => {
                    self.update_tables_in_query(query.as_mut(), database, schema);
                    DFStatement::Statement(Box::new(Statement::Query(query)))
                }
                Statement::CreateTable(create_table_statement) => {
                    if create_table_statement.query.is_some() {
                        #[allow(clippy::unwrap_used)]
                        let mut query = create_table_statement.query.unwrap();
                        self.update_tables_in_query(&mut query, database, schema)?;
                        // TODO: Removing all iceberg properties is temporary solution. It should be
                        // implemented properly in future.
                        // https://github.com/Embucket/control-plane-v2/issues/199
                        let modified_statement = CreateTableStatement {
                            query: Some(query),
                            iceberg: false,
                            base_location: None,
                            external_volume: None,
                            catalog: None,
                            catalog_sync: None,
                            storage_serialization_policy: None,
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
                    self.update_tables_in_table_with_joins(&mut table, database, schema)?;
                    if let Some(from) = from.as_mut() {
                        self.update_tables_in_table_with_joins(from, database, schema)?;
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

    // Fill in the database and schema if they are missing
    // and normalize the identifiers
    #[must_use]
    pub fn resolve_table_ident(&self, mut table_ident: Vec<Ident>) -> ExecutionResult<Vec<Ident>> {
        let database = self.current_database();
        let schema = self.current_schema();
        if table_ident.len() == 1 {
            match (database, schema) {
                (Some(database), Some(schema)) => {
                    table_ident.insert(0, Ident::new(database));
                    table_ident.insert(1, Ident::new(schema));
                }
                (Some(_), None) => {
                    return Err(ExecutionError::InvalidIdentifier {
                        ident: table_ident.iter().map(ToString::to_string).collect().join("."),
                    });
                }
                (None, Some(_)) => {
                    return Err(ExecutionError::InvalidIdentifier {
                        ident: table_ident.iter().map(ToString::to_string).collect().join("."),
                    });
                }
                _ => {}
            }
        } else if table_ident.len() != 3 {
            return plan_err!("Invalid table name: {:?}", table_ident)
        }

        table_ident
            .iter()
            .map(|ident| Ident::new(self.session.ident_normalizer.normalize(ident.clone())))
            .collect()
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

    fn update_tables_in_query(&self, query: &mut Query) -> ExecutionResult<()> {
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                self.update_tables_in_query(&mut cte.query)?;
            }
        }

        match query.body.as_mut() {
            sqlparser::ast::SetExpr::Select(select) => {
                for table_with_joins in &mut select.from {
                    self.update_tables_in_table_with_joins(table_with_joins);
                }

                if let Some(expr) = &mut select.selection {
                    self.update_tables_in_expr(expr)?;
                }
            }
            sqlparser::ast::SetExpr::Query(q) => {
                self.update_tables_in_query(q)?;
            }
            _ => {}
        }

        Ok(())
    }

    fn update_tables_in_expr(&self, expr: &mut Expr) -> ExecutionResult<()> {
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                self.update_tables_in_expr(left)?;
                self.update_tables_in_expr(right)?;
            }
            Expr::Subquery(q) => {
                self.update_tables_in_query(q)?;
            }
            Expr::Exists { subquery, .. } => {
                self.update_tables_in_query(subquery)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn update_tables_in_table_with_joins(
        &self,
        table_with_joins: &mut TableWithJoins,
    ) {
        self.update_tables_in_table_factor(&mut table_with_joins.relation);

        for join in &mut table_with_joins.joins {
            self.update_tables_in_table_factor(&mut join.relation);
        }
    }

    fn update_tables_in_table_factor(&self, table_factor: &mut TableFactor) -> ExecutionResult<()> {
        match table_factor {
            TableFactor::Table { name, .. } => {
                let compressed_name = self.resolve_table_ident(name.0)?;
                *name = ObjectName(compressed_name);
            }
            TableFactor::Derived { subquery, .. } => {
                self.update_tables_in_query(subquery)?;
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.update_tables_in_table_with_joins(table_with_joins)?;
            }
            _ => {}
        }
        Ok(())
    }
}

pub fn created_entity_response() -> ExecutionResult<Vec<RecordBatch>> {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::Int64,
        false,
    )]));
    Ok(vec![RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![0]))],
    ).context(ex_error::ArrowSnafu)?]
    )
}

#[cfg(test)]
mod tests {
    use super::Query;
    use datafusion::sql::parser::DFParser;

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_postprocess_query_statement_functions_expressions() {
        let args: [(&str, &str); 14] = [
            ("select year(ts)", "SELECT date_part('year', ts)"),
            ("select dayofyear(ts)", "SELECT date_part('doy', ts)"),
            ("select day(ts)", "SELECT date_part('day', ts)"),
            ("select dayofmonth(ts)", "SELECT date_part('day', ts)"),
            ("select dayofweek(ts)", "SELECT date_part('dow', ts)"),
            ("select month(ts)", "SELECT date_part('month', ts)"),
            ("select weekofyear(ts)", "SELECT date_part('week', ts)"),
            ("select week(ts)", "SELECT date_part('week', ts)"),
            ("select hour(ts)", "SELECT date_part('hour', ts)"),
            ("select minute(ts)", "SELECT date_part('minute', ts)"),
            ("select second(ts)", "SELECT date_part('second', ts)"),
            ("select minute(ts)", "SELECT date_part('minute', ts)"),
            // Do nothing
            ("select yearofweek(ts)", "SELECT yearofweek(ts)"),
            ("select yearofweekiso(ts)", "SELECT yearofweekiso(ts)"),
        ];

        for (init, exp) in args {
            let statement = DFParser::parse_sql(init).unwrap().pop_front();
            if let Some(mut s) = statement {
                Query::postprocess_query_statement(&mut s);
                assert_eq!(s.to_string(), exp);
            }
        }
    }
}