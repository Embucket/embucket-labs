use crate::error::{self as api_iceberg_rest_error, Operation, Result};
use crate::schemas::{
    CommitTable, GetConfigQuery, from_get_schema, from_schema, from_schemas_list, from_tables_list,
    to_create_table, to_schema, to_table_commit,
};
use crate::state::State as AppState;
use axum::http::StatusCode;
use axum::{Json, extract::Path, extract::Query, extract::State};
use core_metastore::error::{self as metastore_error};
use core_metastore::{ListParams, SchemaIdent as MetastoreSchemaIdent, TableIdent as MetastoreTableIdent};
use core_utils::scan_iterator::ScanIterator;
use iceberg_rest_catalog::models::{
    CatalogConfig, CommitTableResponse, CreateNamespaceRequest, CreateNamespaceResponse,
    CreateTableRequest, GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse,
    LoadTableResult, RegisterTableRequest,
};
use iceberg_rust_spec::table_metadata::TableMetadata;
use object_store::ObjectStore;
use serde_json::{Value, from_slice};
use snafu::ResultExt;
use std::collections::HashMap;
use validator::Validate;

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_namespace(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(schema): Json<CreateNamespaceRequest>,
) -> Result<Json<CreateNamespaceResponse>> {
    let ib_schema = to_schema(schema, database_name);
    let schema = state
        .metastore
        .create_schema(&ib_schema.ident.clone(), ib_schema)
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::CreateNamespace,
        })?;
    Ok(Json(from_schema(schema.data)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_namespace(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> Result<Json<GetNamespaceResponse>> {
    let schema_ident = MetastoreSchemaIdent {
        database: database_name.clone(),
        schema: schema_name.clone(),
    };
    let schema = state
        .metastore
        .get_schema(&schema_ident)
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::GetNamespace,
        })?
        // TBD:
        // - get_schema should return no Option, like this: Result<Schema, core_metastore::Error>
        // - Should we just use Metastore context without artificially creating it?
        // - Only create IcebergAPIError context ?
        .ok_or_else(|| {
            metastore_error::SchemaNotFoundSnafu {
                db: database_name.clone(),
                schema: schema_name.clone(),
            }
            .build()
        })
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::GetNamespace,
        })?;
    Ok(Json(from_get_schema(schema.data)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_namespace(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> Result<StatusCode> {
    let schema_ident = MetastoreSchemaIdent::new(database_name, schema_name);
    state
        .metastore
        .delete_schema(&schema_ident, true)
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::DeleteNamespace,
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_namespaces(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> Result<Json<ListNamespacesResponse>> {
    let schemas = state
        .metastore
        .get_schemas(ListParams::default().by_parent_name(database_name.clone()))
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::ListNamespaces,
        })?;
    Ok(Json(from_schemas_list(schemas)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_table(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(table): Json<CreateTableRequest>,
) -> Result<Json<LoadTableResult>> {
    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &table.name);
    let volume_ident = state
        .metastore
        .volume_for_table(&table_ident.clone())
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::CreateTable,
        })?
        .map(|v| v.data.ident);
    let ib_create_table = to_create_table(table, table_ident.clone(), volume_ident);

    ib_create_table
        .validate()
        .context(metastore_error::ValidationSnafu)
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::CreateTable,
        })?;
    let table = state
        .metastore
        .create_table(&table_ident, ib_create_table)
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::CreateTable,
        })?;
    let mut result = LoadTableResult::new(table.data.metadata);
    result.metadata_location = Some(table.data.metadata_location);
    Ok(Json(result))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn register_table(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(register): Json<RegisterTableRequest>,
) -> Result<Json<LoadTableResult>> {
    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &register.name);
    let metadata_raw = state
        .metastore
        .volume_for_table(&table_ident)
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::RegisterTable,
        })?
        .map(|v| v.data)
        // TBD:
        // - get_volume should return no Option, like this: Result<Schema, core_metastore::Error>
        .ok_or_else(|| {
            metastore_error::VolumeNotFoundSnafu {
                volume: format!(
                    "Volume not found for database {database_name} and schema {schema_name}"
                ),
            }
            .build()
        })
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::RegisterTable,
        })?
        .get_object_store()
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::RegisterTable,
        })?
        .get(&object_store::path::Path::from(
            register.metadata_location.clone(),
        ))
        .await
        .context(metastore_error::ObjectStoreSnafu)
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::RegisterTable,
        })?;
    let metadata_bytes = metadata_raw
        .bytes()
        .await
        .context(metastore_error::ObjectStoreSnafu)
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::RegisterTable,
        })?;
    let table_metadata: TableMetadata = from_slice(&metadata_bytes)
        .context(metastore_error::SerdeSnafu)
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::RegisterTable,
        })?;
    let mut result = LoadTableResult::new(table_metadata);
    result.metadata_location = Some(register.metadata_location);
    Ok(Json(result))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn commit_table(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
    Json(commit): Json<CommitTable>,
) -> Result<Json<CommitTableResponse>> {
    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_name);
    let table_updates = to_table_commit(commit);
    let ib_table = state
        .metastore
        .update_table(&table_ident, table_updates)
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::CommitTable,
        })?;
    Ok(Json(CommitTableResponse::new(
        ib_table.data.metadata_location,
        ib_table.data.metadata,
    )))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_table(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> Result<Json<LoadTableResult>> {
    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_name);
    let table = state
        .metastore
        .get_table(&table_ident)
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::GetTable,
        })?
        .ok_or_else(|| {
            metastore_error::TableNotFoundSnafu {
                db: database_name.clone(),
                schema: schema_name.clone(),
                table: table_name.clone(),
            }
            .build()
        })
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::GetTable,
        })?;
    let mut result = LoadTableResult::new(table.data.metadata);
    result.metadata_location = Some(table.data.metadata_location);
    Ok(Json(result))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_table(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> Result<StatusCode> {
    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_name);
    state
        .metastore
        .delete_table(&table_ident, true)
        .await
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::DeleteTable,
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_tables(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> Result<Json<ListTablesResponse>> {
    let schema_ident = MetastoreSchemaIdent::new(database_name, schema_name);
    let tables = state
        .metastore
        .iter_tables(&schema_ident)
        .collect()
        .await
        .context(metastore_error::UtilSlateDBSnafu)
        .context(api_iceberg_rest_error::MetastoreSnafu {
            operation: Operation::ListTables,
        })?;
    Ok(Json(from_tables_list(tables)))
}

#[tracing::instrument(level = "debug", skip(_state), err, ret(level = tracing::Level::TRACE))]
pub async fn report_metrics(
    State(_state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
    Json(metrics): Json<Value>,
) -> Result<StatusCode> {
    tracing::info!(
        database = %database_name,
        schema = %schema_name,
        table = %table_name,
        ?metrics,
        "Received metrics for table"
    );
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_config(
    State(state): State<AppState>,
    Query(params): Query<GetConfigQuery>,
) -> Result<Json<CatalogConfig>> {
    let catalog_url = state.config.iceberg_catalog_url.clone();
    let config = CatalogConfig {
        defaults: HashMap::new(),
        overrides: HashMap::from([
            ("uri".into(), catalog_url),
            ("prefix".into(), params.warehouse.unwrap_or_default()),
        ]),
        // TODO: I think it can be useful and should be utilized somehow
        endpoints: None,
    };
    Ok(Json(config))
}

// only one endpoint is defined for the catalog implementation to work
// we don't actually have functionality for views yet
#[tracing::instrument(level = "debug", skip(_state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_views(
    State(_state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> Result<Json<ListTablesResponse>> {
    Ok(Json(ListTablesResponse {
        next_page_token: None,
        identifiers: None,
    }))
}
