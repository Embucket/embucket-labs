use super::catalog::information_schema::information_schema::{
    INFORMATION_SCHEMA, InformationSchemaProvider,
};
use super::catalog::{
    catalog_list::EmbucketCatalogList, catalogs::embucket::catalog::EmbucketCatalog,
};
use super::datafusion::planner::ExtendedSqlToRel;
use super::error::{self as ex_error, ExecutionError, ExecutionResult, RefreshCatalogListSnafu};
use super::session::UserSession;
use super::utils::{NormalizedIdent, is_logical_plan_effectively_empty};
use crate::datafusion::rewriters::session_context::SessionContextExprRewriter;
use crate::datafusion::visitors::{copy_into_identifiers, functions_rewriter, json_element};
use crate::models::{QueryContext, QueryResult};
use core_history::history_store::HistoryStore;
use core_metastore::{
    Metastore, SchemaIdent as MetastoreSchemaIdent,
    TableCreateRequest as MetastoreTableCreateRequest, TableFormat as MetastoreTableFormat,
    TableIdent as MetastoreTableIdent,
};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::catalog::MemoryCatalogProvider;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::default_table_source::provider_as_source;
use datafusion::execution::session_state::SessionContextProvider;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::prelude::CsvReadOptions;
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::{CreateExternalTable, DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    CreateTable as CreateTableStatement, Expr, Ident, ObjectName, Query, SchemaName, Statement,
    TableFactor, TableWithJoins,
};
use datafusion::sql::statement::object_name_to_string;
use datafusion_common::{
    DataFusionError, ResolvedTableReference, TableReference, plan_datafusion_err,
};
use datafusion_expr::logical_plan::dml::{DmlStatement, InsertOp, WriteOp};
use datafusion_expr::{CreateMemoryTable, DdlStatement};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use df_catalog::catalog::CachingCatalog;
use df_catalog::information_schema::session_params::SessionProperty;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::spec::arrow::schema::new_fields_with_ids;
use iceberg_rust::spec::namespace::Namespace;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::StructType;
use object_store::aws::AmazonS3Builder;
use snafu::ResultExt;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, GroupByExpr, MergeAction, MergeClauseKind, MergeInsertKind, ObjectNamePart,
    ObjectType, PivotValueSource, Select, SelectItem, ShowObjects, ShowStatementFilter,
    ShowStatementIn, TruncateTableTarget, Use, Value,
};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Write;
use std::sync::Arc;
use tracing_attributes::instrument;
use url::Url;

pub struct UserQuery {
    pub metastore: Arc<dyn Metastore>,
    pub history_store: Arc<dyn HistoryStore>,
    pub raw_query: String,
    pub query: String,
    pub session: Arc<UserSession>,
    pub query_context: QueryContext,
}

pub enum IcebergCatalogResult {
    Catalog(Arc<dyn Catalog>),
    Result(ExecutionResult<QueryResult>),
}

impl UserQuery {
    pub(super) fn new<S>(session: Arc<UserSession>, query: S, query_context: QueryContext) -> Self
    where
        S: Into<String>,
    {
        let query = Self::preprocess_query(&query.into());
        Self {
            metastore: session.metastore.clone(),
            history_store: session.history_store.clone(),
            raw_query: query.clone(),
            query,
            session,
            query_context,
        }
    }

    pub fn parse_query(&self) -> Result<DFStatement, DataFusionError> {
        let state = self.session.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        let mut statement = state.sql_to_statement(&self.raw_query, dialect)?;
        Self::postprocess_query_statement(&mut statement);
        Ok(statement)
    }

    pub async fn plan(&self) -> Result<LogicalPlan, DataFusionError> {
        let statement = self.parse_query()?;
        self.session.ctx.state().statement_to_plan(statement).await
    }

    fn current_database(&self) -> String {
        self.query_context
            .database
            .clone()
            .or_else(|| self.session.get_session_variable("database"))
            .unwrap_or_else(|| "embucket".to_string())
    }

    fn current_schema(&self) -> String {
        self.query_context
            .schema
            .clone()
            .or_else(|| self.session.get_session_variable("schema"))
            .unwrap_or_else(|| "public".to_string())
    }

    async fn refresh_catalog(&self) -> ExecutionResult<()> {
        if let Some(catalog_list_impl) = self
            .session
            .ctx
            .state()
            .catalog_list()
            .as_any()
            .downcast_ref::<EmbucketCatalogList>()
        {
            catalog_list_impl
                .refresh()
                .await
                .context(RefreshCatalogListSnafu)?;
        }
        Ok(())
    }

    fn session_context_expr_rewriter(&self) -> SessionContextExprRewriter {
        let current_database = self.current_database();
        let schemas: Vec<String> = self
            .session
            .ctx
            .catalog(&current_database)
            .map(|c| {
                c.schema_names()
                    .iter()
                    .map(|schema| format!("{current_database}.{schema}"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        SessionContextExprRewriter {
            database: current_database,
            schema: self.current_schema(),
            schemas,
            warehouse: "default".to_string(),
            session_id: self.session.ctx.session_id(),
        }
    }

    #[allow(clippy::unwrap_used)]
    #[instrument(level = "trace", ret)]
    pub fn postprocess_query_statement(statement: &mut DFStatement) {
        if let DFStatement::Statement(value) = statement {
            json_element::visit(value);
            functions_rewriter::visit(value);
            copy_into_identifiers::visit(value);
        }
    }

    #[instrument(level = "debug", skip(self), err, ret(level = tracing::Level::TRACE))]
    pub async fn execute(&mut self) -> ExecutionResult<QueryResult> {
        let statement = self.parse_query().context(super::error::DataFusionSnafu)?;
        self.query = statement.to_string();

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
                        .map(|v| {
                            (
                                v.option_name.clone(),
                                SessionProperty::from_key_value(&v, self.session.ctx.session_id()),
                            )
                        })
                        .collect();

                    self.session.set_session_variable(set, params)?;
                    return self.status_response();
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
                    let params = HashMap::from([(
                        variable.to_string(),
                        SessionProperty::from_str_value(value, Some(self.session.ctx.session_id())),
                    )]);
                    self.session.set_session_variable(true, params)?;
                    return self.status_response();
                }
                Statement::SetVariable {
                    variables, value, ..
                } => {
                    let values: Vec<SessionProperty> = value
                        .iter()
                        .map(|v| match v {
                            Expr::Value(v) => Ok(SessionProperty::from_value(
                                &v.value,
                                self.session.ctx.session_id(),
                            )),
                            _ => Err(ExecutionError::DataFusion {
                                source: DataFusionError::NotImplemented(
                                    "Only primitive statements are supported".to_string(),
                                ),
                            }),
                        })
                        .collect::<Result<_, _>>()?;
                    let params = variables
                        .iter()
                        .map(ToString::to_string)
                        .zip(values.into_iter())
                        .collect();
                    self.session.set_session_variable(true, params)?;
                    return self.status_response();
                }
                Statement::CreateTable { .. } => {
                    return Box::pin(self.create_table_query(*s)).await;
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
                Statement::CreateSchema { .. } => {
                    let result = Box::pin(self.create_schema(*s)).await;
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
                | Statement::Update { .. } => {
                    return Box::pin(self.execute_with_custom_plan(&self.query)).await;
                }
                Statement::ShowDatabases { .. }
                | Statement::ShowSchemas { .. }
                | Statement::ShowTables { .. }
                | Statement::ShowColumns { .. }
                | Statement::ShowViews { .. }
                | Statement::ShowFunctions { .. }
                | Statement::ShowObjects { .. }
                | Statement::ShowVariables { .. }
                | Statement::ShowVariable { .. } => {
                    return Box::pin(self.show_query(*s)).await;
                }
                Statement::Truncate { table_names, .. } => {
                    let result = Box::pin(self.truncate_table(table_names)).await;
                    self.refresh_catalog().await?;
                    return result;
                }
                Statement::Query(mut subquery) => {
                    self.update_qualify_in_query(subquery.as_mut());
                    Self::update_table_result_scan_in_query(subquery.as_mut());
                    self.traverse_and_update_query(subquery.as_mut()).await;
                    return Box::pin(self.execute_with_custom_plan(&subquery.to_string())).await;
                }
                Statement::Drop { .. } => {
                    let result = Box::pin(self.drop_query(*s)).await;
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
    #[instrument(level = "trace", ret)]
    pub fn preprocess_query(query: &str) -> String {
        // TODO: This regex should be a static allocation
        let alter_iceberg_table = regex::Regex::new(r"alter\s+iceberg\s+table").unwrap();
        alter_iceberg_table
            .replace_all(query, "alter table")
            .to_string()
    }

    pub fn get_catalog(&self, name: &str) -> ExecutionResult<Arc<dyn CatalogProvider>> {
        self.session.ctx.state().catalog_list().catalog(name).ok_or(
            ExecutionError::CatalogNotFound {
                catalog: name.to_string(),
            },
        )
    }

    /// The code below relies on [`Catalog`] trait for different iceberg catalog
    /// implementations (REST, S3 table buckets, or anything else).
    /// In case this is built-in datafusion's [`MemoryCatalogProvider`] we shortcut and rely on its implementation
    /// to actually execute logical plan.
    /// Otherwise, code tries to downcast catalog to [`Catalog`] and if successful,
    /// return the catalog
    pub async fn resolve_iceberg_catalog_or_execute(
        &self,
        catalog: Arc<dyn CatalogProvider>,
        catalog_name: String,
        plan: LogicalPlan,
    ) -> IcebergCatalogResult {
        // Try to downcast to CachingCatalog first since all catalogs are registered as CachingCatalog
        let catalog =
            if let Some(caching_catalog) = catalog.as_any().downcast_ref::<CachingCatalog>() {
                &caching_catalog.catalog
            } else {
                return IcebergCatalogResult::Result(Err(ExecutionError::CatalogDownCast {
                    catalog: catalog_name,
                }));
            };

        // Try to resolve the actual underlying catalog type
        if let Some(iceberg_catalog) = catalog.as_any().downcast_ref::<IcebergCatalog>() {
            IcebergCatalogResult::Catalog(iceberg_catalog.catalog())
        } else if let Some(embucket_catalog) = catalog.as_any().downcast_ref::<EmbucketCatalog>() {
            IcebergCatalogResult::Catalog(embucket_catalog.catalog())
        } else if catalog
            .as_any()
            .downcast_ref::<MemoryCatalogProvider>()
            .is_some()
        {
            let result = self.execute_logical_plan(plan).await;
            IcebergCatalogResult::Result(result)
        } else {
            IcebergCatalogResult::Result(Err(ExecutionError::CatalogDownCast {
                catalog: catalog_name,
            }))
        }
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    pub async fn drop_query(&self, statement: Statement) -> ExecutionResult<QueryResult> {
        let Statement::Drop {
            names, object_type, ..
        } = statement.clone()
        else {
            return Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only DROP statements are supported".to_string(),
                ),
            });
        };
        let ident: MetastoreTableIdent = self.resolve_table_object_name(names[0].0.clone())?.into();
        let plan = self.sql_statement_to_plan(statement).await?;
        let catalog = self.get_catalog(ident.database.as_str())?;
        let iceberg_catalog = match self
            .resolve_iceberg_catalog_or_execute(catalog, ident.database.clone(), plan.clone())
            .await
        {
            IcebergCatalogResult::Catalog(catalog) => catalog,
            IcebergCatalogResult::Result(result) => {
                return match result {
                    Ok(_) => self.status_response(),
                    Err(err) => Err(err),
                };
            }
        };

        let table_exists = iceberg_catalog
            .clone()
            .load_tabular(&ident.to_iceberg_ident())
            .await
            .is_ok();
        if table_exists {
            match object_type {
                ObjectType::Table => {
                    iceberg_catalog
                        .drop_table(&ident.to_iceberg_ident())
                        .await
                        .context(ex_error::IcebergSnafu)?;
                }
                ObjectType::View => {
                    iceberg_catalog
                        .drop_view(&ident.to_iceberg_ident())
                        .await
                        .context(ex_error::IcebergSnafu)?;
                }
                _ => {
                    return Err(ExecutionError::DataFusion {
                        source: DataFusionError::NotImplemented(
                            "Only DROP TABLE/VIEW statements are supported".to_string(),
                        ),
                    });
                }
            }
        }
        self.status_response()
    }

    #[allow(clippy::redundant_else, clippy::too_many_lines)]
    #[instrument(level = "trace", skip(self), err, ret)]
    pub async fn create_table_query(&self, statement: Statement) -> ExecutionResult<QueryResult> {
        let Statement::CreateTable(mut create_table_statement) = statement.clone() else {
            return Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only CREATE TABLE statements are supported".to_string(),
                ),
            });
        };
        let table_location = create_table_statement
            .location
            .clone()
            .or_else(|| create_table_statement.base_location.clone());

        let new_table_ident =
            self.resolve_table_object_name(create_table_statement.name.0.clone())?;
        create_table_statement.name = new_table_ident.clone().into();
        // We don't support transient tables for now
        create_table_statement.transient = false;
        // Remove all unsupported iceberg params (we already take them into account)
        create_table_statement.iceberg = false;
        create_table_statement.base_location = None;
        create_table_statement.external_volume = None;
        create_table_statement.catalog = None;
        create_table_statement.catalog_sync = None;
        create_table_statement.storage_serialization_policy = None;

        if let Some(ref mut query) = create_table_statement.query {
            self.update_qualify_in_query(query);
        }

        let plan = self
            .get_custom_logical_plan(&create_table_statement.to_string())
            .await?;
        let ident: MetastoreTableIdent = new_table_ident.into();

        let catalog = self.get_catalog(ident.database.as_str())?;
        self.create_iceberg_table(
            catalog.clone(),
            ident.database.clone(),
            table_location,
            ident.clone(),
            create_table_statement,
            plan.clone(),
        )
        .await?;

        // Now we have created table in the metastore, we need to register it in the catalog
        self.refresh_catalog().await?;

        // Insert data to new table
        // Since we don't execute logical plan, and we don't transform it to physical plan and
        // also don't execute it as well, we need somehow to support CTAS
        if let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
            name,
            input,
            ..
        })) = plan
        {
            if is_logical_plan_effectively_empty(&input) {
                return self.created_entity_response();
            }
            let schema_name = name
                .schema()
                .ok_or(ExecutionError::InvalidSchemaIdentifier {
                    ident: name.to_string(),
                })?;

            let target_table = catalog
                .schema(schema_name)
                .ok_or(ExecutionError::SchemaNotFound {
                    schema: schema_name.to_string(),
                })?
                .table(name.table())
                .await
                .context(super::error::DataFusionSnafu)?
                .ok_or(ExecutionError::TableProviderNotFound {
                    table_name: name.table().to_string(),
                })?;

            let insert_plan = LogicalPlan::Dml(DmlStatement::new(
                name,
                provider_as_source(target_table),
                WriteOp::Insert(InsertOp::Append),
                input,
            ));
            return self.execute_logical_plan(insert_plan).await;
        }
        self.created_entity_response()
    }

    #[allow(unused_variables)]
    pub async fn create_iceberg_table(
        &self,
        catalog: Arc<dyn CatalogProvider>,
        catalog_name: String,
        table_location: Option<String>,
        ident: MetastoreTableIdent,
        statement: CreateTableStatement,
        plan: LogicalPlan,
    ) -> ExecutionResult<QueryResult> {
        let iceberg_catalog = match self
            .resolve_iceberg_catalog_or_execute(catalog, catalog_name, plan.clone())
            .await
        {
            IcebergCatalogResult::Catalog(catalog) => catalog,
            IcebergCatalogResult::Result(result) => {
                return match result {
                    Ok(_) => self.created_entity_response(),
                    Err(err) => Err(err),
                };
            }
        };

        let iceberg_ident = ident.to_iceberg_ident();

        // Check if it already exists, if exists and CREATE OR REPLACE - drop it
        let table_exists = iceberg_catalog
            .clone()
            .load_tabular(&iceberg_ident)
            .await
            .is_ok();

        if table_exists {
            if statement.if_not_exists {
                return self.created_entity_response();
            }

            if statement.or_replace {
                iceberg_catalog
                    .drop_table(&iceberg_ident)
                    .await
                    .context(ex_error::IcebergSnafu)?;
            } else {
                return Err(ExecutionError::ObjectAlreadyExists {
                    type_name: "table".to_string(),
                    name: ident.to_string(),
                });
            }
        }

        let fields_with_ids = StructType::try_from(&new_fields_with_ids(
            plan.schema().as_arrow().fields(),
            &mut 0,
        ))
        .map_err(|err| DataFusionError::External(Box::new(err)))
        .context(super::error::DataFusionSnafu)?;

        // Create builder and configure it
        let mut builder = Schema::builder();
        builder.with_schema_id(0);
        builder.with_identifier_field_ids(vec![]);

        // Add each struct field individually
        for field in fields_with_ids.iter() {
            builder.with_struct_field(field.clone());
        }

        let schema = builder
            .build()
            .map_err(|err| DataFusionError::External(Box::new(err)))
            .context(super::error::DataFusionSnafu)?;

        let mut create_table = CreateTableBuilder::default();
        create_table
            .with_name(ident.table)
            .with_schema(schema)
            // .with_location(location.clone())
            .build(&[ident.schema], iceberg_catalog)
            .await
            .context(ex_error::IcebergSnafu)?;
        self.created_entity_response()
    }

    pub async fn create_external_table_query(
        &self,
        statement: CreateExternalTable,
    ) -> ExecutionResult<QueryResult> {
        let table_location = statement.location.clone();
        let table_format = MetastoreTableFormat::from(statement.file_type);
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
        let table_ident: MetastoreTableIdent =
            self.resolve_table_object_name(statement.name.0)?.into();

        // Create builder and configure it
        let mut builder = Schema::builder();
        builder.with_schema_id(0);
        builder.with_identifier_field_ids(vec![]);

        // Add each struct field individually
        for field in fields_with_ids.iter() {
            builder.with_struct_field(field.clone());
        }

        let table_schema = builder
            .build()
            .map_err(|err| DataFusionError::External(Box::new(err)))
            .context(ex_error::DataFusionSnafu)?;

        let table_create_request = MetastoreTableCreateRequest {
            ident: table_ident.clone(),
            schema: table_schema,
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

        self.created_entity_response()
    }

    /// This is experimental CREATE STAGE support
    /// Current limitations
    /// TODO
    /// - Prepare object storage depending on the URL. Currently we support only s3 public buckets    ///   with public access with default eu-central-1 region
    /// - Parse credentials from specified config
    /// - We don't need to create table in case we have common shared session context.
    ///   CSV is registered as a table which can referenced from SQL statements executed against this context
    /// - Revisit this with the new metastore approach
    pub async fn create_stage_query(&self, statement: Statement) -> ExecutionResult<QueryResult> {
        let Statement::CreateStage {
            name,
            stage_params,
            file_format,
            ..
        } = statement
        else {
            return Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only CREATE STAGE statements are supported".to_string(),
                ),
            });
        };

        let table_name = match name.0.last() {
            Some(ObjectNamePart::Identifier(ident)) => ident.value.clone(),
            _ => {
                return Err(ExecutionError::InvalidTableIdentifier {
                    ident: name.to_string(),
                });
            }
        };

        let skip_header = file_format.options.iter().any(|option| {
            option.option_name.eq_ignore_ascii_case("skip_header")
                && option.value.eq_ignore_ascii_case("1")
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
        let url = Url::parse(file_path.as_str()).map_err(|_| ExecutionError::InvalidFilePath {
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
                table_name,
                file_path,
                CsvReadOptions::new()
                    .has_header(skip_header)
                    .quote(field_optionally_enclosed_by)
                    .schema(&ArrowSchema::new(fields)),
            )
            .await
            .context(ex_error::DataFusionSnafu)?;
        self.status_response()
    }

    pub async fn copy_into_snowflake_query(
        &self,
        statement: Statement,
    ) -> ExecutionResult<QueryResult> {
        let Statement::CopyIntoSnowflake { into, from_obj, .. } = statement else {
            return Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only COPY INTO statements are supported".to_string(),
                ),
            });
        };
        if let Some(from_obj) = from_obj {
            let from_stage: Vec<ObjectNamePart> = from_obj
                .0
                .iter()
                .map(|fs| ObjectNamePart::Identifier(Ident::new(fs.to_string().replace('@', ""))))
                .collect();
            let insert_into = self.resolve_table_object_name(into.0)?;
            let insert_from = self.resolve_table_object_name(from_stage)?;
            // Insert data to table
            let insert_query = format!("INSERT INTO {insert_into} SELECT * FROM {insert_from}");
            self.execute_with_custom_plan(&insert_query).await
        } else {
            Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "FROM object is required for COPY INTO statements".to_string(),
                ),
            })
        }
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    pub async fn merge_query(&self, statement: Statement) -> ExecutionResult<QueryResult> {
        let Statement::Merge {
            table,
            mut source,
            on,
            clauses,
            ..
        } = statement
        else {
            return Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only MERGE statements are supported".to_string(),
                ),
            });
        };

        let (target_table, target_alias) = Self::get_table_with_alias(table);
        let (_source_table, source_alias) = Self::get_table_with_alias(source.clone());

        let target_table = self.resolve_table_object_name(target_table.0)?;

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
        let select_query = format!(
            "SELECT {values} FROM {source_query} LEFT JOIN {target_table} {target_alias} ON {on}{where_clause_str}"
        );

        // Construct the INSERT statement
        let insert_query = format!("INSERT INTO {target_table} ({columns}) {select_query}");
        self.execute_with_custom_plan(&insert_query).await
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    pub async fn create_schema(&self, statement: Statement) -> ExecutionResult<QueryResult> {
        let Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } = statement.clone()
        else {
            return Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only CREATE SCHEMA statements are supported".to_string(),
                ),
            });
        };

        let SchemaName::Simple(schema_name) = schema_name else {
            return Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only simple schema names are supported".to_string(),
                ),
            });
        };

        let ident: MetastoreSchemaIdent = self.resolve_schema_object_name(schema_name.0)?.into();
        let plan = self.sql_statement_to_plan(statement).await?;
        let catalog = self.get_catalog(ident.database.as_str())?;

        let downcast_result = self
            .resolve_iceberg_catalog_or_execute(catalog, ident.database, plan)
            .await;
        let iceberg_catalog = match downcast_result {
            IcebergCatalogResult::Catalog(catalog) => catalog,
            IcebergCatalogResult::Result(result) => {
                return match result {
                    Ok(_) => self.created_entity_response(),
                    Err(err) => Err(err),
                };
            }
        };

        let schema_exists = iceberg_catalog
            .list_namespaces(None)
            .await
            .context(ex_error::IcebergSnafu)?
            .iter()
            .any(|namespace| namespace.join(".") == ident.schema);

        if schema_exists {
            if if_not_exists {
                return self.created_entity_response();
            }
            return Err(ExecutionError::ObjectAlreadyExists {
                type_name: "schema".to_string(),
                name: ident.schema,
            });
        }
        let namespace = Namespace::try_new(&[ident.schema])
            .map_err(|err| DataFusionError::External(Box::new(err)))
            .context(ex_error::DataFusionSnafu)?;
        iceberg_catalog
            .create_namespace(&namespace, None)
            .await
            .context(ex_error::IcebergSnafu)?;
        self.created_entity_response()
    }

    #[allow(clippy::too_many_lines)]
    pub async fn show_query(&self, statement: Statement) -> ExecutionResult<QueryResult> {
        let query = match statement {
            Statement::ShowDatabases { .. } => {
                format!(
                    "SELECT
                        NULL as created_on,
                        database_name as name,
                        'STANDARD' as kind,
                        NULL as database_name,
                        NULL as schema_name
                    FROM {}.information_schema.databases",
                    self.current_database()
                )
            }
            Statement::ShowSchemas { show_options, .. } => {
                let reference = self.resolve_show_in_name(show_options.show_in, false);
                let catalog: String = reference
                    .catalog()
                    .map_or_else(|| self.current_database(), ToString::to_string);
                let sql = format!(
                    "SELECT
                        NULL as created_on,
                        schema_name as name,
                        NULL as kind,
                        catalog_name as database_name,
                        NULL as schema_name
                    FROM {catalog}.information_schema.schemata",
                );
                let mut filters = Vec::new();
                if let Some(filter) =
                    build_starts_with_filter(show_options.starts_with, "schema_name")
                {
                    filters.push(filter);
                }
                apply_show_filters(sql, &filters)
            }
            Statement::ShowTables { show_options, .. } => {
                let reference = self.resolve_show_in_name(show_options.show_in, false);
                let catalog: String = reference
                    .catalog()
                    .map_or_else(|| self.current_database(), ToString::to_string);
                let sql = format!(
                    "SELECT
                        NULL as created_on,
                        table_name as name,
                        table_type as kind,
                        table_catalog as database_name,
                        table_schema as schema_name
                    FROM {catalog}.information_schema.tables"
                );
                let mut filters = vec!["table_type = 'TABLE'".to_string()];
                if let Some(filter) =
                    build_starts_with_filter(show_options.starts_with, "table_name")
                {
                    filters.push(filter);
                }
                if let Some(schema) = reference.schema().filter(|s| !s.is_empty()) {
                    filters.push(format!("table_schema = '{schema}'"));
                }
                apply_show_filters(sql, &filters)
            }
            Statement::ShowViews { show_options, .. } => {
                let reference = self.resolve_show_in_name(show_options.show_in, false);
                let catalog: String = reference
                    .catalog()
                    .map_or_else(|| self.current_database(), ToString::to_string);
                let sql = format!(
                    "SELECT
                        NULL as created_on,
                        view_name as name,
                        view_type as kind,
                        view_catalog as database_name,
                        view_schema as schema_name
                    FROM {catalog}.information_schema.views"
                );
                let mut filters = Vec::new();
                if let Some(filter) =
                    build_starts_with_filter(show_options.starts_with, "view_name")
                {
                    filters.push(filter);
                }
                if let Some(schema) = reference.schema().filter(|s| !s.is_empty()) {
                    filters.push(format!("view_schema = '{schema}'"));
                }
                apply_show_filters(sql, &filters)
            }
            Statement::ShowObjects(ShowObjects { show_options, .. }) => {
                let reference = self.resolve_show_in_name(show_options.show_in, false);
                let catalog: String = reference
                    .catalog()
                    .map_or_else(|| self.current_database(), ToString::to_string);
                let sql = format!(
                    "SELECT
                        NULL as created_on,
                        upper(table_name) as name,
                        table_type as kind,
                        upper(table_catalog) as database_name,
                        upper(table_schema) as schema_name,
                        CASE WHEN table_type='TABLE' then 'Y' else 'N' end as is_iceberg,
                        'N' as is_dynamic
                    FROM {catalog}.information_schema.tables"
                );
                let mut filters = Vec::new();
                if let Some(filter) =
                    build_starts_with_filter(show_options.starts_with, "table_name")
                {
                    filters.push(filter);
                }
                if let Some(schema) = reference.schema().filter(|s| !s.is_empty()) {
                    filters.push(format!("table_schema = '{schema}'"));
                }
                apply_show_filters(sql, &filters)
            }
            Statement::ShowColumns { show_options, .. } => {
                let reference = self.resolve_show_in_name(show_options.show_in.clone(), true);
                let catalog: String = reference
                    .catalog()
                    .map_or_else(|| self.current_database(), ToString::to_string);
                let sql = format!(
                    "SELECT
                        table_name as table_name,
                        table_schema as schema_name,
                        column_name as column_name,
                        data_type as data_type,
                        column_default as default,
                        is_nullable as 'null?',
                        column_type as kind,
                        NULL as expression,
                        table_catalog as database_name,
                        NULL as autoincrement
                    FROM {catalog}.information_schema.columns"
                );
                let mut filters = Vec::new();
                if let Some(filter) =
                    build_starts_with_filter(show_options.starts_with, "column_name")
                {
                    filters.push(filter);
                }
                if let Some(schema) = reference.schema().filter(|s| !s.is_empty()) {
                    filters.push(format!("table_schema = '{schema}'"));
                }
                if show_options.show_in.is_some() {
                    filters.push(format!("table_name = '{}'", reference.table()));
                }
                apply_show_filters(sql, &filters)
            }
            Statement::ShowVariable { variable } => {
                let variable = object_name_to_string(&ObjectName::from(variable));
                format!(
                    "SELECT
                        NULL as created_on,
                        NULL as updated_on,
                        name,
                        value,
                        NULL as type,
                        description as comment
                    FROM {}.information_schema.df_settings
                    WHERE name = '{}'",
                    self.current_database(),
                    variable
                )
            }
            Statement::ShowVariables { filter, .. } => {
                let sql = format!(
                    "SELECT
                        session_id,
                        name AS name,
                        value,
                        type,
                        description as comment,
                        created_on,
                        updated_on,
                    FROM {}.information_schema.df_settings",
                    self.current_database()
                );
                let mut filters = vec!["session_id is NOT NULL".to_string()];
                if let Some(ShowStatementFilter::Like(pattern)) = filter {
                    filters.push(format!("name LIKE '{pattern}'"));
                }
                apply_show_filters(sql, &filters)
            }
            _ => {
                return Err(ExecutionError::DataFusion {
                    source: DataFusionError::NotImplemented(format!(
                        "unsupported SHOW statement: {statement}"
                    )),
                });
            }
        };
        Box::pin(self.execute_with_custom_plan(&query)).await
    }

    pub async fn truncate_table(
        &self,
        table_names: Vec<TruncateTableTarget>,
    ) -> ExecutionResult<QueryResult> {
        let Some(first_table) = table_names.into_iter().next() else {
            return Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "No table names provided for TRUNCATE TABLE".to_string(),
                ),
            });
        };

        let object_name = self.resolve_table_object_name(first_table.name.0)?;
        let mut query = self.session.query(
            format!(
                "CREATE OR REPLACE TABLE {object_name} as (SELECT * FROM {object_name} WHERE FALSE)",
            ),
            QueryContext::default(),
        );
        query.execute().await
    }

    #[must_use]
    pub fn resolve_show_in_name(
        &self,
        show_in: Option<ShowStatementIn>,
        table: bool,
    ) -> TableReference {
        let parts: Vec<String> = show_in
            .and_then(|in_clause| in_clause.parent_name)
            .map(|obj| obj.0.into_iter().map(|ident| ident.to_string()).collect())
            .unwrap_or_default();

        let (catalog, schema, table_name) = match parts.as_slice() {
            [one] if table => (self.current_database(), self.current_schema(), one.clone()),
            [one] => (self.current_database(), one.clone(), String::new()),
            [s, t] if table => (self.current_database(), s.clone(), t.clone()),
            [d, s] => (d.clone(), s.clone(), String::new()),
            [d, s, t] => (d.clone(), s.clone(), t.clone()),
            _ => (self.current_database(), String::new(), String::new()),
        };
        TableReference::full(catalog, schema, table_name)
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    pub async fn get_custom_logical_plan(&self, query: &str) -> ExecutionResult<LogicalPlan> {
        let state = self.session.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();

        // We turn a query to SQL only to turn it back into a statement
        // TODO: revisit this pattern
        let statement = state
            .sql_to_statement(query, dialect)
            .context(super::error::DataFusionSnafu)?;

        if let DFStatement::Statement(s) = statement.clone() {
            self.sql_statement_to_plan(*s).await
        } else {
            Err(ExecutionError::DataFusion {
                source: DataFusionError::NotImplemented(
                    "Only SQL statements are supported".to_string(),
                ),
            })
        }
    }

    pub async fn sql_statement_to_plan(
        &self,
        statement: Statement,
    ) -> ExecutionResult<LogicalPlan> {
        let tables = self
            .table_references_for_statement(
                &DFStatement::Statement(Box::new(statement.clone())),
                &self.session.ctx.state(),
            )
            .await?;
        let ctx_provider = SessionContextProvider {
            state: &self.session.ctx.state(),
            tables,
        };
        let planner =
            ExtendedSqlToRel::new(&ctx_provider, self.session.ctx.state().get_parser_options());
        planner
            .sql_statement_to_plan(statement)
            .context(super::error::DataFusionSnafu)
    }

    async fn execute_sql(&self, query: &str) -> ExecutionResult<QueryResult> {
        let session = self.session.clone();
        let query_id = self.query_context.query_id;
        let query = query.to_string();
        let stream = self
            .session
            .executor
            .spawn(async move {
                let df = session
                    .ctx
                    .sql(&query)
                    .await
                    .context(super::error::DataFusionSnafu)?;
                let schema = df.schema().as_arrow().clone();
                let records = df.collect().await.context(super::error::DataFusionSnafu)?;
                Ok(QueryResult::new(records, Arc::new(schema), query_id))
            })
            .await
            .context(super::error::JobSnafu)??;
        Ok(stream)
    }

    async fn execute_logical_plan(&self, plan: LogicalPlan) -> ExecutionResult<QueryResult> {
        let session = self.session.clone();
        let query_id = self.query_context.query_id;
        let stream = self
            .session
            .executor
            .spawn(async move {
                let schema = plan.schema().as_arrow().clone();
                let records = session
                    .ctx
                    .execute_logical_plan(plan)
                    .await
                    .context(super::error::DataFusionSnafu)?
                    .collect()
                    .await
                    .context(super::error::DataFusionSnafu)?;
                Ok(QueryResult::new(records, Arc::new(schema), query_id))
            })
            .await
            .context(super::error::JobSnafu)??;
        Ok(stream)
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    pub async fn execute_with_custom_plan(&self, query: &str) -> ExecutionResult<QueryResult> {
        let mut plan = self.get_custom_logical_plan(query).await?;
        plan = self
            .session_context_expr_rewriter()
            .rewrite_plan(&plan)
            .context(super::error::DataFusionSnafu)?;
        self.execute_logical_plan(plan).await
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
                            alias: Ident::new("qualify_alias".to_string()),
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
                            projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(
                                Ident::new("*"),
                            ))],
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
                                left: Box::new(Expr::Identifier(Ident::new("qualify_alias"))),
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
                            flavor: sqlparser::ast::SelectFlavor::Standard,
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

    #[allow(clippy::unwrap_used)]
    async fn table_references_for_statement(
        &self,
        statement: &DFStatement,
        state: &SessionState,
    ) -> ExecutionResult<HashMap<ResolvedTableReference, Arc<dyn TableSource>>> {
        let mut tables = HashMap::new();

        let references = state
            .resolve_table_references(statement)
            .context(super::error::DataFusionSnafu)?;
        for reference in references {
            let resolved = state.resolve_table_ref(reference);
            if let Entry::Vacant(v) = tables.entry(resolved.clone()) {
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
        Ok(tables)
    }

    pub fn schema_for_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> datafusion_common::Result<Arc<dyn SchemaProvider>> {
        let resolved_ref = self.session.ctx.state().resolve_table_ref(table_ref);
        if self.session.ctx.state().config().information_schema()
            && *resolved_ref.schema == *INFORMATION_SCHEMA
        {
            return Ok(Arc::new(InformationSchemaProvider::new(
                Arc::clone(self.session.ctx.state().catalog_list()),
                resolved_ref.catalog,
            )));
        }
        self.session
            .ctx
            .state()
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

    fn convert_batches_to_exprs(batches: Vec<RecordBatch>) -> Vec<sqlparser::ast::ExprWithAlias> {
        let mut exprs = Vec::new();
        for batch in batches {
            if batch.num_columns() > 0 {
                let column = batch.column(0);
                for row_idx in 0..batch.num_rows() {
                    if !column.is_null(row_idx) {
                        if let Ok(scalar_value) = ScalarValue::try_from_array(column, row_idx) {
                            let expr = if batch.schema().fields()[0].data_type().is_numeric() {
                                Expr::Value(
                                    Value::Number(scalar_value.to_string(), false)
                                        .with_empty_span(),
                                )
                            } else {
                                Expr::Value(
                                    Value::SingleQuotedString(scalar_value.to_string())
                                        .with_empty_span(),
                                )
                            };

                            exprs.push(sqlparser::ast::ExprWithAlias { expr, alias: None });
                        }
                    }
                }
            }
        }
        exprs
    }

    // This function is used to update the table references in the query
    // It executes the subquery and convert the result to a list of string expressions
    // It then replaces the pivot value source with the list of string expressions
    async fn replace_pivot_subquery_with_list(
        &self,
        table: &TableFactor,
        value_column: &[Ident],
        value_source: &mut PivotValueSource,
    ) {
        match value_source {
            PivotValueSource::Any(order_by_expr) => {
                let col = value_column
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(".");
                let mut query = format!("SELECT DISTINCT {col} FROM {table} ");

                if !order_by_expr.is_empty() {
                    let order_by_clause = order_by_expr
                        .iter()
                        .map(|expr| {
                            let direction = match expr.options.asc {
                                Some(true) => " ASC",
                                Some(false) => " DESC",
                                None => "",
                            };

                            let nulls = match expr.options.nulls_first {
                                Some(true) => " NULLS FIRST",
                                Some(false) => " NULLS LAST",
                                None => "",
                            };

                            format!("{}{}{}", expr.expr, direction, nulls)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    let _ = write!(query, "ORDER BY {order_by_clause}");
                }

                let result = self.execute_with_custom_plan(&query).await;
                if let Ok(batches) = result {
                    *value_source =
                        PivotValueSource::List(Self::convert_batches_to_exprs(batches.records));
                }
            }
            PivotValueSource::Subquery(subquery) => {
                let subquery_sql = subquery.to_string();

                let result = self.execute_with_custom_plan(&subquery_sql).await;

                if let Ok(batches) = result {
                    *value_source =
                        PivotValueSource::List(Self::convert_batches_to_exprs(batches.records));
                }
            }
            PivotValueSource::List(_) => {
                // Do nothing
            }
        }
    }

    /// This method traverses query including set expressions and updates it when needed.
    fn traverse_and_update_query<'a>(
        &'a self,
        query: &'a mut Query,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            fn process_set_expr<'a>(
                this: &'a UserQuery,
                set_expr: &'a mut sqlparser::ast::SetExpr,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
                Box::pin(async move {
                    match set_expr {
                        sqlparser::ast::SetExpr::Select(select) => {
                            for table_with_joins in &mut select.from {
                                if let TableFactor::Pivot {
                                    table,
                                    value_column,
                                    value_source,
                                    ..
                                } = &mut table_with_joins.relation
                                {
                                    this.replace_pivot_subquery_with_list(
                                        table,
                                        value_column,
                                        value_source,
                                    )
                                    .await;
                                }
                            }
                        }

                        sqlparser::ast::SetExpr::Query(inner_query) => {
                            this.traverse_and_update_query(inner_query).await;
                        }
                        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
                            process_set_expr(this, left).await;
                            process_set_expr(this, right).await;
                        }
                        _ => {}
                    }
                })
            }

            process_set_expr(self, &mut query.body).await;

            if let Some(with) = &mut query.with {
                for cte in &mut with.cte_tables {
                    self.traverse_and_update_query(&mut cte.query).await;
                }
            }
        })
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
                        // Extract the database and schema names
                        let db_name = match &name.0[0] {
                            ObjectNamePart::Identifier(ident) => ident.value.clone(),
                        };

                        let schema_name = match &name.0[1] {
                            ObjectNamePart::Identifier(ident) => ident.value.clone(),
                        };

                        Some(TableReference::full(db_name, schema_name, empty()))
                    } else {
                        // Extract just the schema name
                        let schema_name = match &name.0[0] {
                            ObjectNamePart::Identifier(ident) => ident.value.clone(),
                        };

                        Some(TableReference::full(empty(), schema_name, empty()))
                    }
                }
                _ => references.first().cloned(),
            },
            _ => references.first().cloned(),
        }
    }

    // Fill in the database and schema if they are missing
    // and normalize the identifiers for ObjectNamePart
    pub fn resolve_table_object_name(
        &self,
        mut table_ident: Vec<ObjectNamePart>,
    ) -> ExecutionResult<NormalizedIdent> {
        match table_ident.len() {
            1 => {
                table_ident.insert(
                    0,
                    ObjectNamePart::Identifier(Ident::new(self.current_database())),
                );
                table_ident.insert(
                    1,
                    ObjectNamePart::Identifier(Ident::new(self.current_schema())),
                );
            }
            2 => {
                table_ident.insert(
                    0,
                    ObjectNamePart::Identifier(Ident::new(self.current_database())),
                );
            }
            3 => {}
            _ => {
                return Err(ExecutionError::InvalidTableIdentifier {
                    ident: table_ident
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("."),
                });
            }
        }

        let normalized_idents = table_ident
            .iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => {
                    Ident::new(self.session.ident_normalizer.normalize(ident.clone()))
                }
            })
            .collect();

        Ok(NormalizedIdent(normalized_idents))
    }

    // Fill in the database if missing and normalize the identifiers for ObjectNamePart
    pub fn resolve_schema_object_name(
        &self,
        mut schema_ident: Vec<ObjectNamePart>,
    ) -> ExecutionResult<NormalizedIdent> {
        match schema_ident.len() {
            1 => {
                schema_ident.insert(
                    0,
                    ObjectNamePart::Identifier(Ident::new(self.current_database())),
                );
            }
            2 => {}
            _ => {
                return Err(ExecutionError::InvalidSchemaIdentifier {
                    ident: schema_ident
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("."),
                });
            }
        }

        let normalized_idents = schema_ident
            .iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => {
                    Ident::new(self.session.ident_normalizer.normalize(ident.clone()))
                }
            })
            .collect();

        Ok(NormalizedIdent(normalized_idents))
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

    pub fn created_entity_response(&self) -> ExecutionResult<QueryResult> {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            DataType::Int64,
            false,
        )]));
        Ok(QueryResult::new(
            vec![
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![0]))])
                    .context(ex_error::ArrowSnafu)?,
            ],
            schema,
            self.query_context.query_id,
        ))
    }

    pub fn status_response(&self) -> ExecutionResult<QueryResult> {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            false,
        )]));
        Ok(QueryResult::new(
            vec![RecordBatch::new_empty(schema.clone())],
            schema,
            self.query_context.query_id,
        ))
    }
}

fn build_starts_with_filter(starts_with: Option<Value>, column_name: &str) -> Option<String> {
    if let Some(Value::SingleQuotedString(prefix)) = starts_with {
        let escaped = prefix.replace('\'', "''");
        Some(format!("{column_name} LIKE '{escaped}%'"))
    } else {
        None
    }
}

fn apply_show_filters(sql: String, filters: &[String]) -> String {
    if filters.is_empty() {
        sql
    } else {
        format!("{} WHERE {}", sql, filters.join(" AND "))
    }
}
