// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::datasource::default_table_source::provider_as_source;
use datafusion::execution::session_state::SessionContextProvider;
use datafusion::logical_expr::sqlparser::ast::Insert;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::CsvReadOptions;
use datafusion::sql::parser::{CreateExternalTable, DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    CreateTable as CreateTableStatement, Expr, Ident, ObjectName, Query, SchemaName, Statement,
    TableFactor, TableWithJoins,
};
use datafusion_common::{DataFusionError, TableReference};
use datafusion_iceberg::planner::iceberg_transform;
use iceberg_rust::spec::arrow::schema::new_fields_with_ids;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::StructType;
use icebucket_metastore::{
    IceBucketSchema, IceBucketSchemaIdent, IceBucketTableCreateRequest, IceBucketTableFormat,
    IceBucketTableIdent, Metastore,
};
use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    visit_expressions_mut, BinaryOperator, GroupByExpr, MergeAction, MergeClauseKind,
    MergeInsertKind, ObjectType, Query as AstQuery, Select, SelectItem, Use,
};
use sqlparser::tokenizer::Span;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use url::Url;

use super::catalog::IceBucketDFMetastore;
use super::datafusion::functions::visit_functions_expressions;
use super::datafusion::planner::ExtendedSqlToRel;
use super::error::{self as ex_error, ExecutionError, ExecutionResult};
use super::utils::NormalizedIdent;

use super::session::IceBucketUserSession;

#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct IceBucketQueryContext {
    pub database: Option<String>,
    pub schema: Option<String>,
}

pub enum IceBucketQueryState {
    Raw(String),
    Preprocessed(String),
}

pub struct IceBucketQuery {
    pub metastore: Arc<dyn Metastore>,
    pub query: String,
    pub session: Arc<IceBucketUserSession>,
    pub query_context: IceBucketQueryContext,
}

impl IceBucketQuery {
    pub(super) fn new<S>(
        session: Arc<IceBucketUserSession>,
        query: S,
        query_context: IceBucketQueryContext,
    ) -> Self
    where
        S: Into<String>,
    {
        let query = Self::preprocess_query(&query.into());
        Self {
            metastore: session.metastore.clone(),
            query,
            session,
            query_context,
        }
    }

    pub fn parse_query(&self) -> Result<DFStatement, DataFusionError> {
        let state = self.session.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        let mut statement = state.sql_to_statement(&self.query, dialect)?;
        Self::postprocess_query_statement(&mut statement);
        Ok(statement)
    }

    pub async fn plan(&self) -> Result<LogicalPlan, DataFusionError> {
        let statement = self.parse_query()?;
        self.session.ctx.state().statement_to_plan(statement).await
    }

    fn current_database(&self) -> Option<String> {
        self.query_context
            .database
            .clone()
            .or_else(|| self.session.get_session_variable("database"))
            .or_else(|| Some("icebucket".to_string()))
    }

    fn current_schema(&self) -> Option<String> {
        self.query_context
            .schema
            .clone()
            .or_else(|| self.session.get_session_variable("schema"))
            .or_else(|| Some("public".to_string()))
    }

    async fn refresh_catalog(&self) -> ExecutionResult<()> {
        if let Some(catalog_list_impl) = self
            .session
            .ctx
            .state()
            .catalog_list()
            .as_any()
            .downcast_ref::<IceBucketDFMetastore>()
        {
            catalog_list_impl.refresh(&self.session.ctx).await
        } else {
            Err(ExecutionError::RefreshCatalogList {
                message: "Catalog list implementation is not castable".to_string(),
            })
        }
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

    #[tracing::instrument(level = "debug", skip(self), err, ret(level = tracing::Level::TRACE))]
    pub async fn execute(&self) -> ExecutionResult<Vec<RecordBatch>> {
        let mut statement = self.parse_query().context(super::error::DataFusionSnafu)?;
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
                    self.session.set_session_variable(true, params)?;
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
                    self.session.set_session_variable(true, params)?;
                    return Ok(vec![]);
                }
                Statement::CreateTable { .. } => {
                    let result = Box::pin(self.create_table_query(*s)).await;
                    self.refresh_catalog().await?;
                    return result;
                }

                Statement::CreateDatabase { .. } => {
                    // TODO: Databases are only able to be created through the
                    // metastore API. We need to add Snowflake volume syntax to CREATE DATABASE query
                    return Err(ExecutionError::DataFusion {
                        source: DataFusionError::NotImplemented(
                            "Only CREATE TABLE/CREATE SCHEMA statements are supported".to_string(),
                        ),
                    });
                    /*return self
                    .create_database(db_name, if_not_exists)
                    .await;*/
                }
                Statement::CreateSchema {
                    schema_name,
                    if_not_exists,
                } => {
                    let result = self.create_schema(schema_name, if_not_exists).await;
                    self.refresh_catalog().await?;
                    return result;
                }
                Statement::CreateStage { .. } => {
                    // We support only CSV uploads for now
                    let result = Box::pin(self.create_stage_query(*s)).await;
                    self.refresh_catalog().await?;
                    return result;
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
                    return Box::pin(self.execute_with_custom_plan(&self.query)).await;
                }
                Statement::Query(mut subquery) => {
                    self.update_qualify_in_query(subquery.as_mut());
                    Self::update_table_result_scan_in_query(subquery.as_mut());
                    return Box::pin(self.execute_with_custom_plan(&subquery.to_string())).await;
                }
                Statement::Drop { .. } => {
                    let result = Box::pin(self.drop_query(&self.query)).await;
                    self.refresh_catalog().await?;
                    return result;
                }
                Statement::Merge { .. } => {
                    return Box::pin(self.merge_query(*s)).await;
                }
                _ => {}
            }
        } else if let DFStatement::CreateExternalTable(cetable) = statement {
            return Box::pin(self.create_external_table_query(cetable)).await;
        }
        self.execute_sql(&self.query).await
    }

    /// .
    ///
    /// # Panics
    ///
    /// Panics if .
    #[must_use]
    #[allow(clippy::unwrap_used)]
    #[tracing::instrument(level = "trace", ret)]
    pub(super) fn preprocess_query(query: &str) -> String {
        // Replace field[0].subfield -> json_get(json_get(field, 0), 'subfield')
        // TODO: This regex should be a static allocation
        let re = regex::Regex::new(r"(\w+.\w+)\[(\d+)][:\.](\w+)").unwrap();
        //date_add processing moved to `postprocess_query_statement`
        let mut query = re
            .replace_all(query, "json_get(json_get($1, $2), '$3')")
            .to_string();
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
        if let Statement::CreateTable(mut create_table_statement) = statement {
            let new_table_ident =
                self.resolve_table_ident(create_table_statement.name.0.clone())?;
            create_table_statement.name = new_table_ident.clone().into();

            let table_location = create_table_statement
                .location
                .clone()
                .or_else(|| create_table_statement.base_location.clone());

            if let Some(ref mut query) = create_table_statement.query {
                self.update_qualify_in_query(query);
            }

            let session_context = HashMap::new();
            let session_context_planner = SessionContextProvider {
                state: &self.session.ctx.state(),
                tables: session_context,
            };
            let planner = ExtendedSqlToRel::new(
                &session_context_planner,
                self.session.ctx.state().get_parser_options(),
            );
            let plan = planner
                .sql_statement_to_plan(Statement::CreateTable(create_table_statement.clone()))
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
                database: new_table_ident.0[0].to_string(),
                schema: new_table_ident.0[1].to_string(),
                table: new_table_ident.0[2].to_string(),
            };

            let table_exists = self
                .metastore
                .table_exists(&ib_table_ident)
                .await
                .context(ex_error::MetastoreSnafu)?;

            if table_exists && create_table_statement.or_replace {
                self.metastore
                    .delete_table(&ib_table_ident, true)
                    .await
                    .context(ex_error::MetastoreSnafu)?;
            } else if table_exists && create_table_statement.if_not_exists {
                return Err(ExecutionError::ObjectAlreadyExists {
                    type_name: "table".to_string(),
                    name: ib_table_ident.to_string(),
                });
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

            self.metastore
                .create_table(&ib_table_ident, table_create_request)
                .await
                .context(ex_error::MetastoreSnafu)?;

            // Insert data to new table
            // TODO: What is the point of this?
            /*let insert_query = format!("INSERT INTO {ib_table_ident} SELECT * FROM {table_name}",);
            self.execute_with_custom_plan(&insert_query).await?;

            // Drop InMemory table
            let drop_query = format!("DROP TABLE {table_name}");
            self.session
                .ctx
                .sql(&drop_query)
                .await
                .context(super::error::DataFusionSnafu)?
                .collect()
                .await
                .context(super::error::DataFusionSnafu)?;*/

            created_entity_response()
        } else {
            Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only CREATE TABLE statements are supported".to_string(),
                ),
            })
        }
    }

    pub async fn create_external_table_query(
        &self,
        statement: CreateExternalTable,
    ) -> ExecutionResult<Vec<RecordBatch>> {
        let table_location = statement.location.clone();
        let table_format = IceBucketTableFormat::from(statement.file_type);
        let session_context = HashMap::new();
        let session_context_planner = SessionContextProvider {
            state: &self.session.ctx.state(),
            tables: session_context,
        };
        let planner = ExtendedSqlToRel::new(
            &session_context_planner,
            self.session.ctx.state().get_parser_options(),
        );
        let table_schema = planner
            .build_schema(statement.columns)
            .context(ex_error::DataFusionSnafu)?;
        let fields_with_ids =
            StructType::try_from(&new_fields_with_ids(table_schema.fields(), &mut 0))
                .map_err(|err| DataFusionError::External(Box::new(err)))
                .context(ex_error::DataFusionSnafu)?;

        // TODO: Use the options with the table format in the future
        let _table_options = statement.options.clone();
        let table_ident: IceBucketTableIdent = self.resolve_table_ident(statement.name.0)?.into();

        let table_create_request = IceBucketTableCreateRequest {
            ident: table_ident.clone(),
            schema: Schema::builder()
                .with_schema_id(0)
                .with_identifier_field_ids(vec![])
                .with_fields(fields_with_ids)
                .build()
                .map_err(|err| DataFusionError::External(Box::new(err)))
                .context(ex_error::DataFusionSnafu)?,
            location: Some(table_location),
            partition_spec: None,
            sort_order: None,
            stage_create: None,
            volume_ident: None,
            is_temporary: Some(false),
            format: Some(table_format),
            properties: None,
        };

        self.metastore
            .create_table(&table_ident, table_create_request)
            .await
            .context(ex_error::MetastoreSnafu)?;

        created_entity_response()
    }

    /// This is experimental CREATE STAGE support
    /// Current limitations    
    /// TODO
    /// - Prepare object storage depending on the URL. Currently we support only s3 public buckets    ///   with public access with default eu-central-1 region
    /// - Parse credentials from specified config
    /// - We don't need to create table in case we have common shared session context.
    ///   CSV is registered as a table which can referenced from SQL statements executed against this context
    /// - Revisit this with the new metastore approach
    pub(super) async fn create_stage_query(
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
                .ok_or_else(|| ExecutionError::InvalidTableIdentifier {
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
            let url =
                Url::parse(file_path.as_str()).map_err(|_| ExecutionError::InvalidFilePath {
                    path: file_path.clone(),
                })?;
            let bucket = url.host_str().unwrap_or_default();
            // TODO Replace this with the new metastore volume approach
            let s3 = AmazonS3Builder::from_env()
                // TODO Get region automatically from the Volume
                .with_region("eu-central-1")
                .with_bucket_name(bucket)
                .build()
                .map_err(|_| ExecutionError::InvalidBucketIdentifier {
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
            let from_stage: Vec<Ident> = from_stage
                .0
                .iter()
                .map(|fs| Ident::new(fs.to_string().replace('@', "")))
                .collect();
            let insert_into = self.resolve_table_ident(into.0)?;
            let insert_from = self.resolve_table_ident(from_stage)?;
            // Insert data to table
            let insert_query = format!("INSERT INTO {insert_into} SELECT * FROM {insert_from}");
            self.execute_with_custom_plan(&insert_query).await
        } else {
            Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only COPY INTO statements are supported".to_string(),
                ),
            })
        }
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn merge_query(&self, statement: Statement) -> ExecutionResult<Vec<RecordBatch>> {
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

            let target_table = self.resolve_table_ident(target_table.0)?;

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
            self.execute_with_custom_plan(&insert_query).await
        } else {
            Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only MERGE statements are supported".to_string(),
                ),
            })
        }
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn drop_query(&self, query: &str) -> ExecutionResult<Vec<RecordBatch>> {
        // TODO: Parse the query so that the table names can be normalized

        let plan = self.get_custom_logical_plan(query).await?;
        let transformed = plan
            .transform(iceberg_transform)
            .data()
            .context(ex_error::DataFusionSnafu)?;
        let res = self.execute_logical_plan(transformed).await?;
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
                let object_name = self.resolve_schema_ident(schema_name.0)?;

                let database_name = object_name.0[0].clone().to_string();
                let schema_name = object_name.0[1].clone().to_string();

                let icebucket_schema_ident = IceBucketSchemaIdent {
                    database: database_name.clone(),
                    schema: schema_name.clone(),
                };

                let exists = self
                    .metastore
                    .get_schema(&icebucket_schema_ident)
                    .await
                    .context(ex_error::MetastoreSnafu)?
                    .is_some();

                if exists && if_not_exists {
                    return Err(ExecutionError::ObjectAlreadyExists {
                        type_name: "schema".to_string(),
                        name: schema_name.to_string(),
                    });
                } else if !exists {
                    let icebucket_schema = IceBucketSchema {
                        ident: icebucket_schema_ident.clone(),
                        properties: None,
                    };
                    self.metastore
                        .create_schema(&icebucket_schema_ident, icebucket_schema)
                        .await
                        .context(ex_error::MetastoreSnafu)?;
                }
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

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn get_custom_logical_plan(&self, query: &str) -> ExecutionResult<LogicalPlan> {
        let state = self.session.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();

        // We turn a query to SQL only to turn it back into a statement
        // TODO: revisit this pattern
        let mut statement = state
            .sql_to_statement(query, dialect)
            .context(super::error::DataFusionSnafu)?;
        statement = self.update_statement_references(statement)?;

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
                let provider = self
                    .session
                    .ctx
                    .state()
                    .catalog_list()
                    .catalog(&catalog)
                    .unwrap();
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

    async fn execute_sql(&self, query: &str) -> ExecutionResult<Vec<RecordBatch>> {
        let session = self.session.clone();
        let query = query.to_string();
        let stream = self
            .session
            .executor
            .spawn(async move {
                session
                    .ctx
                    .sql(&query)
                    .await
                    .context(super::error::DataFusionSnafu)?
                    .collect()
                    .await
                    .context(super::error::DataFusionSnafu)
            })
            .await
            .context(super::error::JobSnafu)??;
        Ok(stream)
    }

    async fn execute_logical_plan(&self, plan: LogicalPlan) -> ExecutionResult<Vec<RecordBatch>> {
        let session = self.session.clone();
        let stream = self
            .session
            .executor
            .spawn(async move {
                session
                    .ctx
                    .execute_logical_plan(plan)
                    .await
                    .context(super::error::DataFusionSnafu)?
                    .collect()
                    .await
                    .context(super::error::DataFusionSnafu)
            })
            .await
            .context(super::error::JobSnafu)??;
        Ok(stream)
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    pub async fn execute_with_custom_plan(&self, query: &str) -> ExecutionResult<Vec<RecordBatch>> {
        let plan = self.get_custom_logical_plan(query).await?;
        let session = self.session.clone();
        let stream = self
            .session
            .executor
            .spawn(async move {
                session
                    .ctx
                    .execute_logical_plan(plan)
                    .await
                    .context(super::error::DataFusionSnafu)?
                    .collect()
                    .await
                    .context(super::error::DataFusionSnafu)
            })
            .await
            .context(super::error::JobSnafu)??;
        Ok(stream)
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

    // TODO: We need to recursively fix any missing table references with the default
    // database and schema from the session
    /*fn update_statement_table_references(
        &self,
        mut statement: DFStatement,
    ) -> ExecutionResult<DFStatement> {
        match statement {
            DFStatement::Statement(ref mut s) => match **s {
                Statement::Analyze { ref mut table_name, .. } => {
                    *table_name = self.resolve_table_ident(table_name.0.clone())?.into();
                },
                Statement::Truncate { ref mut table_names, .. } => {
                    for table_name in table_names {
                        table_name.name = self.resolve_table_ident(table_name.name.clone().0)?.into();
                    }
                },
                Statement::Msck { ref mut table_name, ..} => {
                    *table_name = self.resolve_table_ident(table_name.0.clone())?.into();
                },
                Statement::Query(query) => todo!(),
                Statement::Insert(insert) => todo!(),
                Statement::Install { extension_name } => todo!(),
                Statement::Load { extension_name } => todo!(),
                Statement::Directory { overwrite, local, path, file_format, source } => todo!(),
                Statement::Call(function) => todo!(),
                Statement::Copy { source, to, target, options, legacy_options, values } => todo!(),
                Statement::CopyIntoSnowflake { into, from_stage, from_stage_alias, stage_params, from_transformations, files, pattern, file_format, copy_options, validation_mode } => todo!(),
                Statement::Close { cursor } => todo!(),
                Statement::Update { table, assignments, from, selection, returning, or } => todo!(),
                Statement::Delete(delete) => todo!(),
                Statement::CreateView { or_replace, materialized, name, columns, query, options, cluster_by, comment, with_no_schema_binding, if_not_exists, temporary, to } => todo!(),
                Statement::CreateTable(create_table) => todo!(),
                Statement::CreateVirtualTable { name, if_not_exists, module_name, module_args } => todo!(),
                Statement::CreateIndex(create_index) => todo!(),
                Statement::CreateRole { names, if_not_exists, login, inherit, bypassrls, password, superuser, create_db, create_role, replication, connection_limit, valid_until, in_role, in_group, role, user, admin, authorization_owner } => todo!(),
                Statement::CreateSecret { or_replace, temporary, if_not_exists, name, storage_specifier, secret_type, options } => todo!(),
                Statement::CreatePolicy { name, table_name, policy_type, command, to, using, with_check } => todo!(),
                Statement::AlterTable { name, if_exists, only, operations, location, on_cluster } => todo!(),
                Statement::AlterIndex { name, operation } => todo!(),
                Statement::AlterView { name, columns, query, with_options } => todo!(),
                Statement::AlterRole { name, operation } => todo!(),
                Statement::AlterPolicy { name, table_name, operation } => todo!(),
                Statement::AlterSession { set, session_params } => todo!(),
                Statement::AttachDatabase { schema_name, database_file_name, database } => todo!(),
                Statement::AttachDuckDBDatabase { if_not_exists, database, database_path, database_alias, attach_options } => todo!(),
                Statement::DetachDuckDBDatabase { if_exists, database, database_alias } => todo!(),
                Statement::Drop { object_type, if_exists, names, cascade, restrict, purge, temporary } => todo!(),
                Statement::DropFunction { if_exists, func_desc, option } => todo!(),
                Statement::DropProcedure { if_exists, proc_desc, option } => todo!(),
                Statement::DropSecret { if_exists, temporary, name, storage_specifier } => todo!(),
                Statement::DropPolicy { if_exists, name, table_name, option } => todo!(),
                Statement::Declare { stmts } => todo!(),
                Statement::CreateExtension { name, if_not_exists, cascade, schema, version } => todo!(),
                Statement::Fetch { name, direction, into } => todo!(),
                Statement::Flush { object_type, location, channel, read_lock, export, tables } => todo!(),
                Statement::Discard { object_type } => todo!(),
                Statement::SetRole { context_modifier, role_name } => todo!(),
                Statement::SetVariable { local, hivevar, variables, value } => todo!(),
                Statement::SetTimeZone { local, value } => todo!(),
                Statement::SetNames { charset_name, collation_name } => todo!(),
                Statement::SetNamesDefault {  } => todo!(),
                Statement::ShowFunctions { filter } => todo!(),
                Statement::ShowVariable { variable } => todo!(),
                Statement::ShowStatus { filter, global, session } => todo!(),
                Statement::ShowVariables { filter, global, session } => todo!(),
                Statement::ShowCreate { obj_type, obj_name } => todo!(),
                Statement::ShowColumns { extended, full, show_options } => todo!(),
                Statement::ShowDatabases { terse, history, show_options } => todo!(),
                Statement::ShowSchemas { terse, history, show_options } => todo!(),
                Statement::ShowObjects { terse, show_options } => todo!(),
                Statement::ShowTables { terse, history, extended, full, external, show_options } => todo!(),
                Statement::ShowViews { terse, materialized, show_options } => todo!(),
                Statement::ShowCollation { filter } => todo!(),
                Statement::Use(_) => todo!(),
                Statement::StartTransaction { modes, begin, transaction, modifier } => todo!(),
                Statement::SetTransaction { modes, snapshot, session } => todo!(),
                Statement::Comment { object_type, object_name, comment, if_exists } => todo!(),
                Statement::Commit { chain } => todo!(),
                Statement::Rollback { chain, savepoint } => todo!(),
                Statement::CreateSchema { schema_name, if_not_exists } => todo!(),
                Statement::CreateDatabase { db_name, if_not_exists, location, managed_location } => todo!(),
                Statement::CreateFunction(create_function) => todo!(),
                Statement::CreateTrigger { or_replace, is_constraint, name, period, events, table_name, referenced_table_name, referencing, trigger_object, include_each, condition, exec_body, characteristics } => todo!(),
                Statement::DropTrigger { if_exists, trigger_name, table_name, option } => todo!(),
                Statement::CreateProcedure { or_alter, name, params, body } => todo!(),
                Statement::CreateMacro { or_replace, temporary, name, args, definition } => todo!(),
                Statement::CreateStage { or_replace, temporary, if_not_exists, name, stage_params, directory_table_params, file_format, copy_options, comment } => todo!(),
                Statement::Assert { condition, message } => todo!(),
                Statement::Grant { privileges, objects, grantees, with_grant_option, granted_by } => todo!(),
                Statement::Revoke { privileges, objects, grantees, granted_by, cascade } => todo!(),
                Statement::Deallocate { name, prepare } => todo!(),
                Statement::Execute { name, parameters, has_parentheses, using } => todo!(),
                Statement::Prepare { name, data_types, statement } => todo!(),
                Statement::Kill { modifier, id } => todo!(),
                Statement::ExplainTable { describe_alias, hive_format, has_table_keyword, table_name } => todo!(),
                Statement::Explain { describe_alias, analyze, verbose, query_plan, statement, format, options } => todo!(),
                Statement::Savepoint { name } => todo!(),
                Statement::ReleaseSavepoint { name } => todo!(),
                Statement::Merge { into, table, source, on, clauses } => todo!(),
                Statement::Cache { table_flag, table_name, has_as, options, query } => todo!(),
                Statement::UNCache { table_name, if_exists } => todo!(),
                Statement::CreateSequence { temporary, if_not_exists, name, data_type, sequence_options, owned_by } => todo!(),
                Statement::CreateType { name, representation } => todo!(),
                Statement::Pragma { name, value, is_eq } => todo!(),
                Statement::LockTables { tables } => todo!(),
                Statement::UnlockTables => todo!(),
                Statement::Unload { query, to, with } => todo!(),
                Statement::OptimizeTable { name, on_cluster, partition, include_final, deduplicate } => todo!(),
                Statement::LISTEN { channel } => todo!(),
                Statement::UNLISTEN { channel } => todo!(),
                Statement::NOTIFY { channel, payload } => todo!(),
                Statement::LoadData { local, inpath, overwrite, table_name, partitioned, table_format } => todo!(),
            },
            DFStatement::CreateExternalTable(create_external_table) => todo!(),
            DFStatement::CopyTo(copy_to_statement) => todo!(),
            DFStatement::Explain(explain_statement) => todo!(),
        };
        Ok(statement)
    }

    fn update_set_expr_table_references(&self, set_expr: &mut SetExpr) {
        match set_expr {
            SetExpr::Select(select) => {
                for table_with_joins in &mut select.from {
                    self.update_table_with_joins(table_with_joins);
                }
            }
            SetExpr::Query(query) => {
                self.update_query_table_references(query);
            }

        }
    }*/

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
        let references = self
            .session
            .ctx
            .state()
            .resolve_table_references(statement)
            .ok()?;

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
    #[allow(clippy::too_many_lines)]
    pub fn update_statement_references(
        &self,
        statement: DFStatement,
    ) -> ExecutionResult<DFStatement> {
        match statement.clone() {
            DFStatement::CreateExternalTable(create_external) => {
                let table_name = self.resolve_table_ident(create_external.name.0)?;
                let modified_statement = CreateExternalTable {
                    name: ObjectName(table_name.0),
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
                    let name = self.resolve_table_ident(name.0)?;
                    let modified_statement = Statement::AlterTable {
                        name: ObjectName(name.0),
                        if_exists,
                        only,
                        operations,
                        location,
                        on_cluster,
                    };
                    Ok(DFStatement::Statement(Box::new(modified_statement)))
                }
                Statement::Insert(insert_statement) => {
                    let table_name = self.resolve_table_ident(insert_statement.table_name.0)?;

                    let source = insert_statement.source.map(|mut query| {
                        self.update_tables_in_query(query.as_mut())
                            .map(|()| Some(Box::new(AstQuery { ..*query })))
                    });

                    let source = if let Some(source) = source {
                        source?
                    } else {
                        None
                    };

                    let modified_statement = Insert {
                        table_name: ObjectName(table_name.0),
                        source,
                        ..insert_statement
                    };
                    Ok(DFStatement::Statement(Box::new(Statement::Insert(
                        modified_statement,
                    ))))
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
                    for name in &mut names {
                        match object_type {
                            ObjectType::Schema => {
                                *name = ObjectName(self.resolve_schema_ident(name.0.clone())?.0);
                            }
                            ObjectType::Table => {
                                *name = ObjectName(self.resolve_table_ident(name.0.clone())?.0);
                            }
                            _ => {}
                        }
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
                    Ok(DFStatement::Statement(Box::new(modified_statement)))
                }
                Statement::Query(mut query) => {
                    self.update_tables_in_query(query.as_mut())?;
                    Ok(DFStatement::Statement(Box::new(Statement::Query(query))))
                }
                Statement::CreateTable(create_table_statement) => {
                    if create_table_statement.query.is_some() {
                        #[allow(clippy::unwrap_used)]
                        let mut query = create_table_statement.query.unwrap();
                        self.update_tables_in_query(&mut query)?;
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
                        Ok(DFStatement::Statement(Box::new(Statement::CreateTable(
                            modified_statement,
                        ))))
                    } else {
                        Ok(statement)
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
                    self.update_tables_in_table_with_joins(&mut table)?;
                    if let Some(from) = from.as_mut() {
                        self.update_tables_in_table_with_joins(from)?;
                    }
                    let modified_statement = Statement::Update {
                        table,
                        assignments,
                        from,
                        selection,
                        returning,
                        or,
                    };
                    Ok(DFStatement::Statement(Box::new(modified_statement)))
                }
                _ => Ok(statement),
            },
            _ => Ok(statement),
        }
    }

    // Fill in the database and schema if they are missing
    // and normalize the identifiers
    pub fn resolve_table_ident(
        &self,
        mut table_ident: Vec<Ident>,
    ) -> ExecutionResult<NormalizedIdent> {
        let database = self.current_database();
        let schema = self.current_schema();
        if table_ident.len() == 1 {
            match (database, schema) {
                (Some(database), Some(schema)) => {
                    table_ident.insert(0, Ident::new(database));
                    table_ident.insert(1, Ident::new(schema));
                }
                (Some(_), None) | (None, Some(_)) => {
                    return Err(ExecutionError::InvalidTableIdentifier {
                        ident: NormalizedIdent(table_ident).to_string(),
                    });
                }
                _ => {}
            }
        } else if table_ident.len() != 3 {
            return Err(ExecutionError::InvalidTableIdentifier {
                ident: NormalizedIdent(table_ident).to_string(),
            });
        }

        Ok(NormalizedIdent(
            table_ident
                .iter()
                .map(|ident| Ident::new(self.session.ident_normalizer.normalize(ident.clone())))
                .collect(),
        ))
    }

    pub fn resolve_schema_ident(
        &self,
        mut schema_ident: Vec<Ident>,
    ) -> ExecutionResult<NormalizedIdent> {
        let database = self.current_database();
        if schema_ident.len() == 1 {
            if let Some(database) = database {
                schema_ident.insert(0, Ident::new(database));
            } else {
                return Err(ExecutionError::InvalidSchemaIdentifier {
                    ident: NormalizedIdent(schema_ident).to_string(),
                });
            }
        } else {
            return Err(ExecutionError::InvalidSchemaIdentifier {
                ident: NormalizedIdent(schema_ident).to_string(),
            });
        }

        Ok(NormalizedIdent(
            schema_ident
                .iter()
                .map(|ident| Ident::new(self.session.ident_normalizer.normalize(ident.clone())))
                .collect(),
        ))
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
                    self.update_tables_in_table_with_joins(table_with_joins)?;
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
    ) -> ExecutionResult<()> {
        self.update_tables_in_table_factor(&mut table_with_joins.relation)?;

        for join in &mut table_with_joins.joins {
            self.update_tables_in_table_factor(&mut join.relation)?;
        }

        Ok(())
    }

    fn update_tables_in_table_factor(&self, table_factor: &mut TableFactor) -> ExecutionResult<()> {
        match table_factor {
            TableFactor::Table { name, .. } => {
                let compressed_name = self.resolve_table_ident(name.clone().0)?;
                *name = ObjectName(compressed_name.0);
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
    )
    .context(ex_error::ArrowSnafu)?])
}
