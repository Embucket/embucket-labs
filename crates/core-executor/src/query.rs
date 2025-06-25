use super::catalog::information_schema::information_schema::{
    INFORMATION_SCHEMA, InformationSchemaProvider,
};
use super::catalog::{
    catalog_list::EmbucketCatalogList, catalogs::embucket::catalog::EmbucketCatalog,
};
use super::datafusion::planner::ExtendedSqlToRel;
use super::error::{self as ex_error, Error, RefreshCatalogListSnafu, Result};
use super::session::UserSession;
use super::utils::{NormalizedIdent, is_logical_plan_effectively_empty};
use crate::datafusion::logical_plan::merge::MergeIntoSink;
use crate::datafusion::rewriters::session_context::SessionContextExprRewriter;
use crate::models::{QueryContext, QueryResult};
use core_history::HistoryStore;
use core_metastore::{
    Metastore, SchemaIdent as MetastoreSchemaIdent,
    TableCreateRequest as MetastoreTableCreateRequest, TableFormat as MetastoreTableFormat,
    TableIdent as MetastoreTableIdent,
};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::MemoryCatalogProvider;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::DefaultTableSource;
use datafusion::datasource::default_table_source::provider_as_source;
use datafusion::execution::session_state::SessionContextProvider;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{self, col};
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::prelude::{CsvReadOptions, DataFrame};
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::{CreateExternalTable, Statement as DFStatement};
use datafusion::sql::planner::ParserOptions;
use datafusion::sql::resolve::resolve_table_references;
use datafusion::sql::sqlparser::ast::{
    CreateTable as CreateTableStatement, Expr, Ident, ObjectName, Query, SchemaName, Statement,
    TableFactor, TableWithJoins,
};
use datafusion::sql::statement::object_name_to_string;
use datafusion_common::{
    DFSchema, DataFusionError, ResolvedTableReference, SchemaReference, TableReference,
    plan_datafusion_err,
};
use datafusion_expr::logical_plan::dml::{DmlStatement, InsertOp, WriteOp};
use datafusion_expr::planner::ContextProvider;
use datafusion_expr::{
    CreateMemoryTable, DdlStatement, Expr as DFExpr, Extension, JoinType, LogicalPlanBuilder,
    Projection, TryCast, and, build_join_schema, is_null, lit, or, when,
};
use datafusion_iceberg::DataFusionTable;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use df_catalog::catalog::CachingCatalog;
use df_catalog::catalog_list::CachedEntity;
use df_catalog::error::Error as CatalogError;
use df_catalog::information_schema::session_params::SessionProperty;
use df_catalog::table::CachingTable;
use embucket_functions::semi_structured::variant::visitors::visit_all;
use embucket_functions::visitors::{
    copy_into_identifiers, fetch_to_limit, functions_rewriter, inline_aliases_in_query,
    json_element, select_expr_aliases, table_functions, top_limit,
    unimplemented::functions_checker::visit as unimplemented_functions_checker,
};
use iceberg_rust::catalog::Catalog;
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::spec::arrow::schema::new_fields_with_ids;
use iceberg_rust::spec::namespace::Namespace;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::StructType;
use object_store::aws::AmazonS3Builder;
use snafu::ResultExt;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, GroupByExpr, MergeAction, MergeClause, MergeClauseKind,
    MergeInsertKind, ObjectNamePart, ObjectType, PivotValueSource, Select, SelectItem, ShowObjects,
    ShowStatementFilter, ShowStatementIn, TruncateTableTarget, Use, Value, visit_relations_mut,
};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::ops::ControlFlow;
use std::sync::Arc;
use tracing_attributes::instrument;
use url::Url;

static SOURCE_EXISTS: &str = "__source_exists";
static TARGET_EXISTS: &str = "__target_exists";

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
    Result(Result<QueryResult>),
}

impl UserQuery {
    pub(super) fn new<S>(session: Arc<UserSession>, query: S, query_context: QueryContext) -> Self
    where
        S: Into<String> + Clone,
    {
        Self {
            metastore: session.metastore.clone(),
            history_store: session.history_store.clone(),
            raw_query: query.clone().into(),
            query: query.into(),
            session,
            query_context,
        }
    }

    pub fn parse_query(&self) -> std::result::Result<DFStatement, DataFusionError> {
        let state = self.session.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        let mut statement = state.sql_to_statement(&self.raw_query, dialect)?;
        // it is designed to return DataFusionError, this is the reason why we
        // create DataFusionError manually. This is also requires manual unboxing.
        Self::postprocess_query_statement_with_validation(&mut statement).map_err(|e| match e {
            Error::DataFusion { error, .. } => *error,
            _ => DataFusionError::NotImplemented(e.to_string()),
        })?;
        Ok(statement)
    }

    pub async fn plan(&self) -> std::result::Result<LogicalPlan, DataFusionError> {
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

    #[instrument(
        name = "UserQuery::refresh_catalog_partially",
        level = "debug",
        skip(self),
        err
    )]
    async fn refresh_catalog_partially(&self, entity: CachedEntity) -> Result<()> {
        if let Some(catalog_list_impl) = self
            .session
            .ctx
            .state()
            .catalog_list()
            .as_any()
            .downcast_ref::<EmbucketCatalogList>()
        {
            let res = catalog_list_impl
                .invalidate_cache(entity)
                .await
                .context(RefreshCatalogListSnafu);

            // Not sure we need this, but just in case
            // TODO: Remove following block when refresh_catalog removed
            if let Err(Error::RefreshCatalogList { source, .. }) = res {
                // refresh entire catalog if cache error happen
                if let CatalogError::InvalidCache { .. } = *source {
                    self.refresh_catalog().await?;
                }
            }
        }
        Ok(())
    }

    async fn refresh_catalog(&self) -> Result<()> {
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
            version: self.session.config.embucket_version.clone(),
            query_context: self.query_context.clone(),
            history_store: self.history_store.clone(),
        }
    }

    #[instrument(name = "UserQuery::postprocess_query_statement", level = "trace", err)]
    pub fn postprocess_query_statement_with_validation(statement: &mut DFStatement) -> Result<()> {
        if let DFStatement::Statement(value) = statement {
            json_element::visit(value);
            functions_rewriter::visit(value);
            top_limit::visit(value);
            unimplemented_functions_checker(value).context(ex_error::UnimplementedFunctionSnafu)?;
            copy_into_identifiers::visit(value);
            select_expr_aliases::visit(value);
            inline_aliases_in_query::visit(value);
            fetch_to_limit::visit(value).context(ex_error::SqlParserSnafu)?;
            table_functions::visit(value);
            visit_all(value);
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[instrument(
        name = "UserQuery::execute",
        level = "debug",
        skip(self),
        fields(statement),
        err
    )]
    pub async fn execute(&mut self) -> Result<QueryResult> {
        let statement = self.parse_query().context(ex_error::DataFusionSnafu)?;
        self.query = statement.to_string();

        // Record the result as part of the current span.
        tracing::Span::current().record("statement", format!("{statement:#?}"));

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
                        return ex_error::OnyUseWithVariablesSnafu.fail();
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
                            _ => ex_error::OnlyPrimitiveStatementsSnafu.fail(),
                        })
                        .collect::<std::result::Result<_, _>>()?;
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
                    return ex_error::OnlyTableSchemaCreateStatementsSnafu.fail();
                    /*return self
                    .create_database(db_name, if_not_exists)
                    .await;*/
                }
                Statement::CreateSchema { .. } => {
                    let result = Box::pin(self.create_schema(*s)).await;
                    return result;
                }
                Statement::CreateStage { .. } => {
                    // We support only CSV uploads for now
                    let result = Box::pin(self.create_stage_query(*s)).await;
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
                    return result;
                }
                Statement::Query(mut subquery) => {
                    self.update_qualify_in_query(subquery.as_mut());
                    self.traverse_and_update_query(subquery.as_mut()).await;
                    return Box::pin(self.execute_with_custom_plan(&subquery.to_string())).await;
                }
                Statement::Drop { .. } => {
                    let result = Box::pin(self.drop_query(*s)).await;
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

    #[instrument(name = "UserQuery::get_catalog", level = "trace", skip(self), err)]
    pub fn get_catalog(&self, name: &str) -> Result<Arc<dyn CatalogProvider>> {
        self.session
            .ctx
            .state()
            .catalog_list()
            .catalog(name)
            .ok_or_else(|| {
                ex_error::CatalogNotFoundSnafu {
                    catalog: name.to_string(),
                }
                .build()
            })
    }

    /// The code below relies on [`Catalog`] trait for different iceberg catalog
    /// implementations (REST, S3 table buckets, or anything else).
    /// In case this is built-in datafusion's [`MemoryCatalogProvider`] we shortcut and rely on its implementation
    /// to actually execute logical plan.
    /// Otherwise, code tries to downcast catalog to [`Catalog`] and if successful,
    /// return the catalog
    #[instrument(
        name = "UserQuery::resolve_iceberg_catalog_or_execute",
        level = "trace",
        skip(self, catalog)
    )]
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
                return IcebergCatalogResult::Result(
                    ex_error::CatalogDownCastSnafu {
                        catalog: catalog_name,
                    }
                    .fail(),
                );
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
            IcebergCatalogResult::Result(
                ex_error::CatalogDownCastSnafu {
                    catalog: catalog_name,
                }
                .fail(),
            )
        }
    }

    #[instrument(name = "UserQuery::drop_query", level = "trace", skip(self), err)]
    pub async fn drop_query(&self, statement: Statement) -> Result<QueryResult> {
        let Statement::Drop { object_type, .. } = statement.clone() else {
            return ex_error::OnlyDropStatementsSnafu.fail();
        };

        let plan = self.sql_statement_to_plan(statement).await?;
        let table_ref = match plan {
            LogicalPlan::Ddl(ref ddl) => match ddl {
                DdlStatement::DropTable(t) => self.resolve_table_ref(t.name.clone()),
                DdlStatement::DropView(v) => self.resolve_table_ref(v.name.clone()),
                DdlStatement::DropCatalogSchema(s) => self.resolve_schema_ref(s.name.clone()),
                _ => return ex_error::OnlyDropStatementsSnafu.fail(),
            },
            _ => return ex_error::OnlyDropStatementsSnafu.fail(),
        };

        let catalog_name = table_ref.catalog.as_ref();
        let schema_name = table_ref.schema.to_string();
        let ident = Identifier::new(&[schema_name.clone()], table_ref.table.as_ref());

        let catalog = self.get_catalog(catalog_name)?;
        let iceberg_catalog = match self
            .resolve_iceberg_catalog_or_execute(catalog, catalog_name.to_string(), plan)
            .await
        {
            IcebergCatalogResult::Catalog(catalog) => catalog,
            IcebergCatalogResult::Result(result) => {
                return result.map(|_| self.status_response())?;
            }
        };

        match object_type {
            ObjectType::Table | ObjectType::View => {
                if iceberg_catalog.clone().load_tabular(&ident).await.is_ok() {
                    iceberg_catalog
                        .drop_table(&ident)
                        .await
                        .context(ex_error::IcebergSnafu)?;
                    self.refresh_catalog_partially(CachedEntity::Table(MetastoreTableIdent {
                        database: catalog_name.to_string(),
                        schema: schema_name,
                        table: ident.name().to_string(),
                    }))
                    .await?;
                }
                self.status_response()
            }
            ObjectType::Schema => {
                let namespace = ident.namespace();
                if iceberg_catalog
                    .clone()
                    .namespace_exists(namespace)
                    .await
                    .is_ok()
                {
                    iceberg_catalog
                        .drop_namespace(namespace)
                        .await
                        .context(ex_error::IcebergSnafu)?;
                    self.refresh_catalog_partially(CachedEntity::Schema(MetastoreSchemaIdent {
                        database: catalog_name.to_string(),
                        schema: schema_name,
                    }))
                    .await?;
                }
                self.status_response()
            }
            _ => ex_error::OnlyDropStatementsSnafu.fail(),
        }
    }

    #[allow(clippy::redundant_else, clippy::too_many_lines)]
    #[instrument(
        name = "UserQuery::create_table_query",
        level = "trace",
        skip(self),
        err
    )]
    pub async fn create_table_query(&self, statement: Statement) -> Result<QueryResult> {
        let Statement::CreateTable(mut create_table_statement) = statement.clone() else {
            return ex_error::OnlyCreateTableStatementsSnafu.fail();
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
        create_table_statement.cluster_by = None;

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
        self.refresh_catalog_partially(CachedEntity::Table(ident))
            .await?;

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
            let schema_name = name.schema().ok_or_else(|| {
                ex_error::InvalidSchemaIdentifierSnafu {
                    ident: name.to_string(),
                }
                .build()
            })?;

            let target_table = catalog
                .schema(schema_name)
                .ok_or_else(|| {
                    ex_error::SchemaNotFoundSnafu {
                        schema: schema_name.to_string(),
                    }
                    .build()
                })?
                .table(name.table())
                .await
                .context(ex_error::DataFusionSnafu)?
                .ok_or_else(|| {
                    ex_error::TableProviderNotFoundSnafu {
                        table_name: name.table().to_string(),
                    }
                    .build()
                })?;
            let schema = target_table.schema();
            let insert_plan = LogicalPlan::Dml(DmlStatement::new(
                name,
                provider_as_source(target_table),
                WriteOp::Insert(InsertOp::Append),
                cast_input_to_target_schema(input, &schema)?,
            ));
            return self.execute_logical_plan(insert_plan).await;
        }
        self.created_entity_response()
    }

    #[allow(unused_variables)]
    #[instrument(
        name = "UserQuery::create_iceberg_table",
        level = "trace",
        skip(self),
        err
    )]
    pub async fn create_iceberg_table(
        &self,
        catalog: Arc<dyn CatalogProvider>,
        catalog_name: String,
        table_location: Option<String>,
        ident: MetastoreTableIdent,
        statement: CreateTableStatement,
        plan: LogicalPlan,
    ) -> Result<QueryResult> {
        let iceberg_catalog = match self
            .resolve_iceberg_catalog_or_execute(catalog, catalog_name, plan.clone())
            .await
        {
            IcebergCatalogResult::Catalog(catalog) => catalog,
            IcebergCatalogResult::Result(result) => {
                return result.map(|_| self.created_entity_response())?;
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
                return ex_error::ObjectAlreadyExistsSnafu {
                    type_name: "table".to_string(),
                    name: ident.to_string(),
                }
                .fail();
            }
        }

        let fields_with_ids = StructType::try_from(&new_fields_with_ids(
            plan.schema().as_arrow().fields(),
            &mut 0,
        ))
        .map_err(|err| DataFusionError::External(Box::new(err)))
        .context(ex_error::DataFusionSnafu)?;

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
            .context(ex_error::DataFusionSnafu)?;

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

    #[instrument(
        name = "UserQuery::create_external_table_query",
        level = "trace",
        skip(self),
        err
    )]
    pub async fn create_external_table_query(
        &self,
        statement: CreateExternalTable,
    ) -> Result<QueryResult> {
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
    #[instrument(
        name = "UserQuery::create_stage_query",
        level = "trace",
        skip(self),
        err
    )]
    pub async fn create_stage_query(&self, statement: Statement) -> Result<QueryResult> {
        let Statement::CreateStage {
            name,
            stage_params,
            file_format,
            ..
        } = statement
        else {
            return ex_error::OnlyCreateStageStatementsSnafu.fail();
        };

        let table_name = match name.0.last() {
            Some(ObjectNamePart::Identifier(ident)) => ident.value.clone(),
            _ => {
                return ex_error::InvalidTableIdentifierSnafu {
                    ident: name.to_string(),
                }
                .fail();
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
        let url = Url::parse(file_path.as_str()).map_err(|_| {
            ex_error::InvalidFilePathSnafu {
                path: file_path.clone(),
            }
            .build()
        })?;
        let bucket = url.host_str().unwrap_or_default();
        // TODO Replace this with the new metastore volume approach
        let s3 = AmazonS3Builder::from_env()
            // TODO Get region automatically from the Volume
            .with_region("eu-central-1")
            .with_bucket_name(bucket)
            .build()
            .map_err(|_| {
                ex_error::InvalidBucketIdentifierSnafu {
                    ident: bucket.to_string(),
                }
                .build()
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

    #[instrument(
        name = "UserQuery::copy_into_snowflake_query",
        level = "trace",
        skip(self),
        err
    )]
    pub async fn copy_into_snowflake_query(&self, statement: Statement) -> Result<QueryResult> {
        let Statement::CopyIntoSnowflake { into, from_obj, .. } = statement else {
            return ex_error::OnlyCopyIntoStatementsSnafu.fail();
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
            ex_error::FromObjectRequiredForCopyIntoStatementsSnafu.fail()
        }
    }

    #[allow(clippy::too_many_lines)]
    #[instrument(name = "UserQuery::merge_query", level = "trace", skip(self), err)]
    pub async fn merge_query(&self, statement: Statement) -> Result<QueryResult> {
        let Statement::Merge {
            table: target,
            source,
            on,
            clauses,
            ..
        } = statement
        else {
            return ex_error::OnlyMergeStatementsSnafu.fail();
        };
        if !matches!(
            &target,
            TableFactor::Table {
                name: _,
                alias: _,
                args: _,
                with_hints: _,
                version: _,
                with_ordinality: _,
                partitions: _,
                json_path: _,
                sample: _,
                index_hints: _
            }
        ) {
            return ex_error::MergeTargetMustBeTableSnafu.fail();
        }
        if !matches!(
            &source,
            TableFactor::Table {
                name: _,
                alias: _,
                args: _,
                with_hints: _,
                version: _,
                with_ordinality: _,
                partitions: _,
                json_path: _,
                sample: _,
                index_hints: _
            }
        ) {
            return ex_error::MergeSourceMustBeTableSnafu.fail();
        }

        let df_session_state = self.session.ctx.state();

        let (target_table, target_alias) = Self::get_table_with_alias(target);
        let (source_table, source_alias) = Self::get_table_with_alias(source.clone());

        let target_table = self.resolve_table_object_name(target_table.0)?;
        let source_table = self.resolve_table_object_name(source_table.0)?;

        let target_provider = {
            let target_cache = self
                .session
                .ctx
                .table_provider(&target_table)
                .await
                .context(ex_error::DataFusionSnafu)?;

            target_cache
                .as_any()
                .downcast_ref::<CachingTable>()
                .ok_or_else(|| {
                    ex_error::CatalogDownCastSnafu {
                        catalog: "DataFusionTable".to_string(),
                    }
                    .build()
                })?
                .table
                .clone()
        };

        let target = DataFusionTable {
            branch: None,
            ..target_provider
                .as_any()
                .downcast_ref::<DataFusionTable>()
                .ok_or_else(|| ex_error::MergeTargetMustBeIcebergTableSnafu.build())?
                .clone()
        };

        let target_table_source: Arc<dyn TableSource> =
            Arc::new(DefaultTableSource::new(Arc::new(target.clone())));

        let target_plan = DataFrame::new(
            df_session_state.clone(),
            LogicalPlanBuilder::scan(&target_table, target_table_source.clone(), None)
                .context(ex_error::DataFusionSnafu)?
                .alias(&target_alias)
                .context(ex_error::DataFusionSnafu)?
                .build()
                .context(ex_error::DataFusionSnafu)?,
        )
        .with_column(TARGET_EXISTS, lit(true))
        .context(ex_error::DataFusionSnafu)?
        .into_unoptimized_plan();

        let target_schema = target_plan.schema().clone();

        let source_provider = self
            .session
            .ctx
            .table_provider(&target_table)
            .await
            .context(ex_error::DataFusionSnafu)?;

        let source_table_source: Arc<dyn TableSource> =
            Arc::new(DefaultTableSource::new(source_provider));

        let source_plan = DataFrame::new(
            df_session_state.clone(),
            LogicalPlanBuilder::scan(&source_table, source_table_source.clone(), None)
                .context(ex_error::DataFusionSnafu)?
                .alias(&source_alias)
                .context(ex_error::DataFusionSnafu)?
                .build()
                .context(ex_error::DataFusionSnafu)?,
        )
        .with_column(SOURCE_EXISTS, lit(true))
        .context(ex_error::DataFusionSnafu)?
        .into_unoptimized_plan();

        let source_schema = source_plan.schema().clone();

        let tables = HashMap::from_iter(vec![
            (self.resolve_table_ref(&target_table), target_table_source),
            (self.resolve_table_ref(&source_table), source_table_source),
        ]);

        let session_context_planner = SessionContextProvider {
            state: &df_session_state,
            tables,
        };
        let sql_planner = ExtendedSqlToRel::new(&session_context_planner, ParserOptions::default());
        let mut planner_context = datafusion::sql::planner::PlannerContext::new();

        let schema = build_join_schema(&target_schema, &source_schema, &JoinType::Full)
            .context(ex_error::DataFusionSnafu)?;

        let on_expr = sql_planner
            .as_ref()
            .sql_to_expr((*on).clone(), &schema, &mut planner_context)
            .context(ex_error::DataFusionSnafu)?;

        let merge_clause_projection =
            merge_clause_projection(&sql_planner, &target_schema, &source_schema, clauses)?;

        let join_plan = LogicalPlanBuilder::new(target_plan)
            .join_on(source_plan, JoinType::Full, [on_expr; 1])
            .context(ex_error::DataFusionSnafu)?
            .project(merge_clause_projection)
            .context(ex_error::DataFusionSnafu)?
            .build()
            .context(ex_error::DataFusionSnafu)?;

        let merge_into_plan =
            MergeIntoSink::new(Arc::new(join_plan), target).context(ex_error::DataFusionSnafu)?;

        self.execute_logical_plan(LogicalPlan::Extension(Extension {
            node: Arc::new(merge_into_plan),
        }))
        .await
    }

    #[instrument(name = "UserQuery::create_schema", level = "trace", skip(self), err)]
    pub async fn create_schema(&self, statement: Statement) -> Result<QueryResult> {
        let Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } = statement.clone()
        else {
            return ex_error::OnlyCreateSchemaStatementsSnafu.fail();
        };

        let SchemaName::Simple(schema_name) = schema_name else {
            return ex_error::OnlySimpleSchemaNamesSnafu.fail();
        };

        let ident: MetastoreSchemaIdent = self.resolve_schema_object_name(schema_name.0)?.into();
        let plan = self.sql_statement_to_plan(statement).await?;
        let catalog = self.get_catalog(&ident.database)?;

        let downcast_result = self
            .resolve_iceberg_catalog_or_execute(catalog, ident.database.clone(), plan)
            .await;
        let iceberg_catalog = match downcast_result {
            IcebergCatalogResult::Catalog(catalog) => catalog,
            IcebergCatalogResult::Result(result) => {
                return result.map(|_| self.created_entity_response())?;
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
            return ex_error::ObjectAlreadyExistsSnafu {
                type_name: "schema".to_string(),
                name: ident.schema,
            }
            .fail();
        }
        let namespace = Namespace::try_new(&[ident.schema.clone()])
            .map_err(|err| DataFusionError::External(Box::new(err)))
            .context(ex_error::DataFusionSnafu)?;
        iceberg_catalog
            .create_namespace(&namespace, None)
            .await
            .context(ex_error::IcebergSnafu)?;

        self.refresh_catalog_partially(CachedEntity::Schema(ident))
            .await?;

        self.created_entity_response()
    }

    #[allow(clippy::too_many_lines)]
    pub async fn show_query(&self, statement: Statement) -> Result<QueryResult> {
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
                        is_iceberg,
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
                return ex_error::UnsupportedShowStatementSnafu {
                    statement: statement.to_string(),
                }
                .fail();
            }
        };
        Box::pin(self.execute_with_custom_plan(&query)).await
    }

    #[instrument(
        name = "UserQuery::truncate_table",
        level = "trace",
        skip(self),
        err,
        ret
    )]
    pub async fn truncate_table(
        &self,
        table_names: Vec<TruncateTableTarget>,
    ) -> Result<QueryResult> {
        let Some(first_table) = table_names.into_iter().next() else {
            return ex_error::NoTableNamesForTruncateTableSnafu {}.fail();
        };

        let object_name = self.resolve_table_object_name(first_table.name.0)?;
        let mut query = self.session.query(
            format!(
                "CREATE OR REPLACE TABLE {object_name} as (SELECT * FROM {object_name} WHERE FALSE)",
            ),
            QueryContext::default(),
        );
        let res = query.execute().await;

        let table_ident: MetastoreTableIdent = object_name.into();

        self.refresh_catalog_partially(CachedEntity::Table(table_ident))
            .await?;

        res
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

    #[instrument(
        name = "UserQuery::get_custom_logical_plan",
        level = "trace",
        skip(self),
        err,
        ret
    )]
    pub async fn get_custom_logical_plan(&self, query: &str) -> Result<LogicalPlan> {
        let state = self.session.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();

        // We turn a query to SQL only to turn it back into a statement
        // TODO: revisit this pattern
        let mut statement = state
            .sql_to_statement(query, dialect)
            .context(ex_error::DataFusionSnafu)?;
        self.update_statement_references(&mut statement)?;

        if let DFStatement::Statement(s) = statement.clone() {
            self.sql_statement_to_plan(*s).await
        } else {
            ex_error::OnlySQLStatementsSnafu.fail()
        }
    }

    /// Fully qualifies all table references in the provided SQL statement.
    ///
    /// This function traverses the SQL statement and updates table references to their fully qualified
    /// names (including catalog and schema), based on the current session context and catalog state.
    ///
    /// - Table references that are part of Common Table Expressions (CTEs) are skipped and left as-is.
    /// - Table functions (recognized by the session context) are also skipped and left unresolved.
    /// - All other table references are resolved using `resolve_table_object_name` and updated in-place.
    ///
    /// # Arguments
    /// * `statement` - The SQL statement (`DFStatement`) to update.
    ///
    /// # Errors
    /// Returns an error if table resolution fails for any non-CTE, non-table-function reference.
    pub fn update_statement_references(&self, statement: &mut DFStatement) -> Result<()> {
        let (_tables, ctes) = resolve_table_references(
            statement,
            self.session
                .ctx
                .state()
                .config()
                .options()
                .sql_parser
                .enable_ident_normalization,
        )
        .context(ex_error::DataFusionSnafu)?;
        match statement {
            DFStatement::Statement(stmt) => {
                let cte_names: HashSet<String> = ctes
                    .into_iter()
                    .map(|cte| cte.table().to_string())
                    .collect();

                let _ = visit_relations_mut(stmt, |table_name: &mut ObjectName| {
                    let is_table_func = self
                        .session
                        .ctx
                        .table_function(table_name.to_string().to_ascii_lowercase().as_str())
                        .is_ok();
                    if !cte_names.contains(&table_name.to_string()) && !is_table_func {
                        match self.resolve_table_object_name(table_name.0.clone()) {
                            Ok(resolved_name) => {
                                *table_name = ObjectName::from(resolved_name.0);
                            }
                            Err(e) => return ControlFlow::Break(e),
                        }
                    }
                    ControlFlow::Continue(())
                });
                Ok(())
            }
            // If needed, handle other DFStatement variants like CreateExternalTable similarly
            _ => Ok(()),
        }
    }

    pub async fn sql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
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
            .context(ex_error::DataFusionSnafu)
    }

    async fn execute_sql(&self, query: &str) -> Result<QueryResult> {
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
                    .context(ex_error::DataFusionSnafu)?;
                let mut schema = df.schema().as_arrow().clone();
                let records = df.collect().await.context(ex_error::DataFusionSnafu)?;
                if !records.is_empty() {
                    schema = records[0].schema().as_ref().clone();
                }
                Ok::<QueryResult, Error>(QueryResult::new(records, Arc::new(schema), query_id))
            })
            .await
            .context(ex_error::JobSnafu)??;
        Ok(stream)
    }

    async fn execute_logical_plan(&self, plan: LogicalPlan) -> Result<QueryResult> {
        let session = self.session.clone();
        let query_id = self.query_context.query_id;
        let stream = self
            .session
            .executor
            .spawn(async move {
                let mut schema = plan.schema().as_arrow().clone();
                let records = session
                    .ctx
                    .execute_logical_plan(plan)
                    .await
                    .context(ex_error::DataFusionSnafu)?
                    .collect()
                    .await
                    .context(ex_error::DataFusionSnafu)?;
                if !records.is_empty() {
                    schema = records[0].schema().as_ref().clone();
                }
                Ok::<QueryResult, Error>(QueryResult::new(records, Arc::new(schema), query_id))
            })
            .await
            .context(ex_error::JobSnafu)??;
        Ok(stream)
    }

    #[instrument(
        name = "UserQuery::execute_with_custom_plan",
        level = "trace",
        skip(self),
        err
    )]
    pub async fn execute_with_custom_plan(&self, query: &str) -> Result<QueryResult> {
        let mut plan = self.get_custom_logical_plan(query).await?;
        plan = self
            .session_context_expr_rewriter()
            .rewrite_plan(&plan)
            .context(ex_error::DataFusionSnafu)?;
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
    ) -> Result<HashMap<ResolvedTableReference, Arc<dyn TableSource>>> {
        let mut tables = HashMap::new();

        let references = state
            .resolve_table_references(statement)
            .context(ex_error::DataFusionSnafu)?;
        for reference in references {
            let resolved = self.resolve_table_ref(reference);
            if let Entry::Vacant(v) = tables.entry(resolved.clone()) {
                if let Ok(schema) = self.schema_for_ref(resolved.clone()) {
                    if let Some(table) = schema
                        .table(&resolved.table)
                        .await
                        .context(ex_error::DataFusionSnafu)?
                    {
                        v.insert(provider_as_source(table));
                    }
                }
            }
        }
        Ok(tables)
    }

    /// Resolves a [`TableReference`] to a [`ResolvedTableReference`]
    /// using the default catalog and schema.
    pub fn resolve_table_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> ResolvedTableReference {
        table_ref
            .into()
            .resolve(&self.current_database(), &self.current_schema())
    }

    #[must_use]
    pub fn resolve_schema_ref(&self, schema: SchemaReference) -> ResolvedTableReference {
        match schema {
            SchemaReference::Bare { schema } => ResolvedTableReference {
                catalog: Arc::from(self.current_database()),
                schema,
                table: Arc::from(""),
            },
            SchemaReference::Full { catalog, schema } => ResolvedTableReference {
                catalog,
                schema,
                table: Arc::from(""),
            },
        }
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
    #[instrument(
        name = "UserQuery::resolve_table_object_name",
        level = "trace",
        skip(self),
        err
    )]
    pub fn resolve_table_object_name(
        &self,
        mut table_ident: Vec<ObjectNamePart>,
    ) -> Result<NormalizedIdent> {
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
                return ex_error::InvalidTableIdentifierSnafu {
                    ident: table_ident
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("."),
                }
                .fail();
            }
        }

        let normalized_idents = table_ident
            .into_iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => self.normalize_ident(ident),
            })
            .collect();
        Ok(NormalizedIdent(normalized_idents))
    }

    // Fill in the database if missing and normalize the identifiers for ObjectNamePart
    pub fn resolve_schema_object_name(
        &self,
        mut schema_ident: Vec<ObjectNamePart>,
    ) -> Result<NormalizedIdent> {
        match schema_ident.len() {
            1 => {
                schema_ident.insert(
                    0,
                    ObjectNamePart::Identifier(Ident::new(self.current_database())),
                );
            }
            2 => {}
            _ => {
                return ex_error::InvalidSchemaIdentifierSnafu {
                    ident: schema_ident
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("."),
                }
                .fail();
            }
        }
        let normalized_idents = schema_ident
            .into_iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => self.normalize_ident(ident),
            })
            .collect();
        Ok(NormalizedIdent(normalized_idents))
    }

    fn normalize_ident(&self, ident: Ident) -> Ident {
        match ident.quote_style {
            Some(qs) => Ident::with_quote(qs, ident.value),
            None => Ident::new(self.session.ident_normalizer.normalize(ident)),
        }
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

    pub fn created_entity_response(&self) -> Result<QueryResult> {
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

    pub fn status_response(&self) -> Result<QueryResult> {
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

/// Converts merge clauses into projection expressions for copy-on-write operations.
///
/// This function processes MERGE statement clauses (UPDATE/INSERT) and generates `DataFusion`
/// projection expressions that compute the new table state. Each column gets an expression
/// that determines its new value based on the merge operation type:
/// - Operation code 3: Matched rows (UPDATE)
/// - Operation code 2: Not matched rows (INSERT)
///
/// # Arguments
/// * `sql_planner` - SQL to logical expression planner
/// * `schema` - Target table schema
/// * `merge_clause` - Vector of merge clauses to process
///
/// # Returns
/// Vector of expressions for each column in the target schema
pub fn merge_clause_projection<S: ContextProvider>(
    sql_planner: &ExtendedSqlToRel<'_, S>,
    target_schema: &DFSchema,
    source_schema: &DFSchema,
    merge_clause: Vec<MergeClause>,
) -> ExecutionResult<Vec<logical_expr::Expr>> {
    let mut updates: HashMap<String, (logical_expr::Expr, logical_expr::Expr)> = HashMap::new();
    let mut inserts: HashMap<String, (logical_expr::Expr, logical_expr::Expr)> = HashMap::new();

    let mut planner_context = datafusion::sql::planner::PlannerContext::new();

    for merge_clause in merge_clause {
        let op = match merge_clause.clause_kind {
            MergeClauseKind::Matched => Ok(and(col(TARGET_EXISTS), col(SOURCE_EXISTS))),
            MergeClauseKind::NotMatched => {
                Ok(or(is_null(col(TARGET_EXISTS)), is_null(col(TARGET_EXISTS))))
            }
            MergeClauseKind::NotMatchedByTarget => Ok(is_null(col(TARGET_EXISTS))),
            MergeClauseKind::NotMatchedBySource => {
                return Err(ex_error::NotMatchedBySourceNotSupportedSnafu.build());
            }
        }?;
        match merge_clause.action {
            MergeAction::Update { assignments } => {
                for assignment in assignments {
                    match assignment.target {
                        AssignmentTarget::ColumnName(column) => {
                            let column_name = column.to_string();
                            let expr = sql_planner
                                .as_ref()
                                .sql_to_expr(assignment.value, source_schema, &mut planner_context)
                                .context(ex_error::DataFusionSnafu)?;
                            updates.insert(column_name, (op.clone(), expr));
                        }
                        AssignmentTarget::Tuple(_) => todo!(),
                    }
                }
            }
            MergeAction::Insert(insert) => {
                let MergeInsertKind::Values(values) = insert.kind else {
                    return Err(ex_error::OnlyMergeStatementsSnafu.build());
                };
                if values.rows.len() != 1 {
                    return Err(ex_error::MergeInsertOnlyOneRowSnafu.build());
                }
                for (column, value) in insert
                    .columns
                    .iter()
                    .zip(values.rows.into_iter().next().unwrap().into_iter())
                {
                    let column_name = column.value.clone();
                    let expr = sql_planner
                        .as_ref()
                        .sql_to_expr(value, source_schema, &mut planner_context)
                        .context(ex_error::DataFusionSnafu)?;
                    inserts.insert(column_name, (op.clone(), expr));
                }
            }
            _ => (),
        }
    }
    let mut exprs: Vec<datafusion_expr::Expr> = target_schema
        .iter()
        .map(|(table_reference, field)| {
            let name = table_reference
                .map(|x| x.to_string() + ".")
                .unwrap_or_default()
                + field.name();
            let update = updates.remove(field.name());
            let insert = inserts.remove(field.name());

            // If there is no update or insert, do nothing
            if update.is_none() && insert.is_none() {
                return Ok(col(name));
            }

            let case_expr = match (update, insert) {
                (Some((update_when, update_then)), Some((insert_when, insert_then))) => {
                    when(update_when, update_then)
                        .when(insert_when, insert_then)
                        .otherwise(col(name))?
                }
                (Some((w, t)), None) | (None, Some((w, t))) => when(w, t).otherwise(col(name))?,
                (None, None) => col(name),
            }
            .alias(field.name().clone());

            Ok::<_, DataFusionError>(case_expr)
        })
        .collect::<Result<_, DataFusionError>>()
        .context(ex_error::DataFusionSnafu)?;
    exprs.push(col(SOURCE_EXISTS));
    Ok(exprs)
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

pub fn cast_input_to_target_schema(
    input: Arc<LogicalPlan>,
    target_schema: &SchemaRef,
) -> Result<Arc<LogicalPlan>> {
    let input_schema = input.schema().as_arrow();
    let mut projections: Vec<DFExpr> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let name = field.name();
        let data_type = field.data_type();
        let input_field = input_schema
            .field_with_name(name)
            .context(ex_error::ArrowSnafu)?;
        if input_field.data_type() == data_type {
            projections.push(col(name));
        } else {
            projections.push(DFExpr::TryCast(TryCast::new(
                Box::new(col(name)),
                data_type.clone(),
            )));
        }
    }
    let projection = Projection::try_new(projections, input).context(ex_error::DataFusionSnafu)?;
    Ok(Arc::new(LogicalPlan::Projection(projection)))
}
