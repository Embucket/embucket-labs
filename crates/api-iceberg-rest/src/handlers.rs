use crate::error::{
    IcebergAPIError, IcebergAPIResult, MetastoreOperationSnafu, RequestProcessingFailedSnafu, // Import Snafu context selectors
    ApiIcebergRestErrorKind, SerializationSnafu, ValidationSnafu // Other error contexts
};
use crate::schemas::{
    CommitTable, GetConfigQuery, from_get_schema, from_schema, from_schemas_list, from_tables_list,
    to_create_table, to_schema, to_table_commit,
};
use crate::state::State as AppState;
use axum::http::StatusCode;
use axum::{Json, extract::Path, extract::Query, extract::State};
use core_metastore::error::{self as cm_error, MetastoreError, MetastoreErrorKind}; // Use cm_error for disambiguation
use core_metastore::{SchemaIdent as MetastoreSchemaIdent, TableIdent as MetastoreTableIdent, Metastore}; // Import trait
use core_utils::scan_iterator::ScanIterator; // Ensure this is Result-aware or handled
use embucket_errors::wrap_error; // Import wrap_error
use iceberg_rest_catalog::models::{
    CatalogConfig, CommitTableResponse, CreateNamespaceRequest, CreateNamespaceResponse,
    CreateTableRequest, GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse,
    LoadTableResult, RegisterTableRequest,
};
use iceberg_rust_spec::table_metadata::TableMetadata;
use object_store::ObjectStore;
use serde_json::{Value, from_slice};
use snafu::ResultExt; // For .context()
use std::collections::HashMap;
use validator::Validate;


// Helper to map MetastoreError to IcebergAPIError::MetastoreOperation
fn map_metastore_error(metastore_err: MetastoreError, api_context_message: String) -> IcebergAPIError {
    let embucket_metastore_err = wrap_error(metastore_err, api_context_message);
    MetastoreOperationSnafu { source: embucket_metastore_err }.build()
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_namespace(
    State(state): State<AppState>,
    Path(database_name): Path<String>, // This database_name from path is for URL structure, actual ns is in payload
    Json(request_payload): Json<CreateNamespaceRequest>,
) -> IcebergAPIResult<Json<CreateNamespaceResponse>> {
    // Validate that path database_name matches payload if strictness is needed, or choose one source.
    // Iceberg spec: "namespace" (array of strings) is in payload. Path is for routing.
    // For now, assume database_name from path is a warehouse prefix or similar, not part of the namespace itself.
    // The actual namespace is in request_payload.namespace.
    if request_payload.namespace.is_empty() {
        let kind = ApiIcebergRestErrorKind::InvalidRequestParameter {
            name: "namespace".to_string(),
            reason: "Namespace array cannot be empty.".to_string(),
        };
        let embucket_err = wrap_error(kind, "Invalid create namespace request".to_string());
        return RequestProcessingFailedSnafu { source: embucket_err }.fail();
    }
    // Assuming the first part of the namespace array is the database, and the second is the schema.
    // This interpretation might need to align with how namespaces are structured project-wide.
    // For a 2-level namespace (db.schema):
    let (db_name_from_payload, schema_name_from_payload) = match request_payload.namespace.as_slice() {
        [db, sch] => (db.clone(), sch.clone()),
        _ => {
            let kind = ApiIcebergRestErrorKind::InvalidRequestParameter {
                name: "namespace".to_string(),
                reason: "Namespace must be a two-part identifier (database.schema).".to_string(),
            };
            let embucket_err = wrap_error(kind, "Invalid namespace structure in request".to_string());
            return RequestProcessingFailedSnafu { source: embucket_err }.fail();
        }
    };

    let schema_ident = MetastoreSchemaIdent {
        database: db_name_from_payload.clone(),
        schema: schema_name_from_payload.clone(),
    };
    let metastore_schema_payload = MetastoreSchema { // Assuming models::Schema is the right type
        ident: schema_ident.clone(),
        properties: request_payload.properties,
    };

    let created_schema_rwo = state
        .metastore
        .create_schema(&schema_ident, metastore_schema_payload)
        .await
        .map_err(|e| map_metastore_error(e, format!("API: Failed to create namespace '{}.{}'", db_name_from_payload, schema_name_from_payload)))?;
    
    Ok(Json(from_schema(created_schema_rwo.data))) // from_schema needs RwObject<Schema> or Schema
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_namespace(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>, // These are parts of the namespace
) -> IcebergAPIResult<Json<GetNamespaceResponse>> {
    let schema_ident = MetastoreSchemaIdent {
        database: database_name.clone(),
        schema: schema_name.clone(),
    };

    let schema_rwo_opt = state
        .metastore
        .get_schema(&schema_ident)
        .await
        .map_err(|e| map_metastore_error(e, format!("API: Failed to query namespace '{}.{}'", database_name, schema_name)))?;

    let schema_rwo = schema_rwo_opt.ok_or_else(|| {
        let kind = MetastoreErrorKind::SchemaNotFound { db: database_name.clone(), schema: schema_name.clone() };
        let metastore_err = cm_error::OperationFailedSnafu { source: wrap_error(kind, "Schema does not exist in metastore".to_string()) }.build();
        map_metastore_error(metastore_err, format!("API: Namespace '{}.{}' not found", database_name, schema_name))
    })?;
    
    Ok(Json(from_get_schema(schema_rwo.data)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_namespace(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> IcebergAPIResult<StatusCode> {
    let schema_ident = MetastoreSchemaIdent::new(database_name.clone(), schema_name.clone());
    state
        .metastore
        .delete_schema(&schema_ident, true) // cascade delete
        .await
        .map_err(|e| map_metastore_error(e, format!("API: Failed to delete namespace '{}.{}'", database_name, schema_name)))?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_namespaces(
    State(state): State<AppState>,
    Path(database_name): Path<String>, // This is parent_namespace, e.g., a database
) -> IcebergAPIResult<Json<ListNamespacesResponse>> {
    // Assuming database_name from path is the "database" part of the namespace
    // Iceberg namespaces can be multi-level. Here, we list schemas within a database.
    let schemas_rwos = state
        .metastore
        .iter_schemas(&database_name) // Pass database_name as filter
        .map_err(|e| map_metastore_error(e, format!("API: Failed to list namespaces under '{}'", database_name)))?
        .collect::<cm_error::MetastoreResult<Vec<_>>>() // Assuming iter_schemas returns MetastoreResult<Iterator>
        .await // And collect is async and returns MetastoreResult<Vec>
        .map_err(|e| map_metastore_error(e, format!("API: Failed to collect namespaces from iterator for '{}'", database_name)))?;
    
    let schemas_data = schemas_rwos.into_iter().map(|rwo| rwo.data).collect();
    Ok(Json(from_schemas_list(schemas_data)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_table(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(table_req_payload): Json<CreateTableRequest>,
) -> IcebergAPIResult<Json<LoadTableResult>> {
    table_req_payload.validate().map_err(|e| // Handle validation errors from the request payload itself
        ValidationSnafu{ source: wrap_error(e, "Invalid CreateTableRequest payload".to_string()) }.build()
    )?;

    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_req_payload.name);
    
    // Determine volume_ident. This logic might be complex depending on how volumes are assigned.
    // For simplicity, let's assume it might come from a config or a default.
    // If it needs to be derived via metastore.volume_for_table, that call needs to be made carefully.
    // For now, to_create_table might not need volume_ident if it's resolved by metastore.create_table
    let volume_ident = state.metastore.volume_for_table(&table_ident).await
        .map(|v_rwo| Some(v_rwo.data.ident)) // Get Option<VolumeIdent>
        .or_else(|e| { // If volume_for_table failed (e.g. DB not found), decide if it's fatal or if table creation can proceed (e.g. for temp tables)
            // This depends on how create_table in metastore handles missing dependent entities for default locations.
            // For now, let's propagate the error if volume_for_table fails.
            Err(map_metastore_error(e, format!("API: Failed to determine volume for table '{}.{}.{}'", database_name, schema_name, table_req_payload.name)))
        })?;


    let ib_create_table_req = to_create_table(table_req_payload, table_ident.clone(), volume_ident);

    // ib_create_table_req.validate() already called above for the raw payload.
    // If to_create_table does significant transformations, its output might also need validation.

    let created_table_rwo = state
        .metastore
        .create_table(&table_ident, ib_create_table_req)
        .await
        .map_err(|e| map_metastore_error(e, format!("API: Failed to create table '{}.{}.{}'", database_name, schema_name, table_ident.table)))?;
    
    Ok(Json(LoadTableResult::new(created_table_rwo.data.metadata)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn register_table(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>, // Namespace where table is registered
    Json(register_req_payload): Json<RegisterTableRequest>,
) -> IcebergAPIResult<Json<LoadTableResult>> {
    register_req_payload.validate().map_err(|e|
        ValidationSnafu{ source: wrap_error(e, "Invalid RegisterTableRequest payload".to_string()) }.build()
    )?;

    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &register_req_payload.name);
    
    // Get the object store for the table's target volume/database.
    // This assumes that the table, once registered, will belong to a volume determined by its namespace.
    let object_store = state.metastore.table_object_store(&table_ident).await.map_err(|e|
        map_metastore_error(e, format!("API: Failed to get object store for registering table '{}.{}.{}'", database_name, schema_name, register_req_payload.name))
    )?;
    
    let metadata_location_path = object_store::path::Path::from(register_req_payload.metadata_location.as_str());
    let metadata_raw = object_store.get(&metadata_location_path).await
        .map_err(|e_os| wrap_error(e_os, format!("Failed to get metadata file '{}' from object store", register_req_payload.metadata_location)))
        .context(crate::error::ObjectStoreInteractionSnafu)?; // Specific API error variant for OS issues

    let metadata_bytes = metadata_raw.bytes().await
        .map_err(|e_os| wrap_error(e_os, format!("Failed to read bytes from metadata file '{}'", register_req_payload.metadata_location)))
        .context(crate::error::ObjectStoreInteractionSnafu)?;
        
    let table_metadata: TableMetadata = from_slice(&metadata_bytes)
        .map_err(|e_serde| wrap_error(e_serde, "Failed to deserialize table metadata".to_string()))
        .context(SerializationSnafu)?;

    // Here, one might create/update the table in the metastore if registration implies that.
    // The current code only loads and returns metadata. If it should also persist:
    // let create_req = TableCreateRequest { name: table_ident.table, location: Some(table_metadata.location().to_string()), ... metadata fields ... };
    // state.metastore.create_table(...) or update_table.
    // For now, assume register is just validating metadata access.

    Ok(Json(LoadTableResult::new(table_metadata)))
}


#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn commit_table(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
    Json(commit_payload): Json<CommitTable>, // CommitTable is alias for CommitTableRequest
) -> IcebergAPIResult<Json<CommitTableResponse>> {
    commit_payload.validate().map_err(|e|
        ValidationSnafu{ source: wrap_error(e, "Invalid CommitTableRequest payload".to_string()) }.build()
    )?;

    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_name);
    let table_updates = to_table_commit(commit_payload); // Converts to core_metastore::TableUpdate

    let updated_table_rwo = state
        .metastore
        .update_table(&table_ident, table_updates)
        .await
        .map_err(|e| map_metastore_error(e, format!("API: Failed to commit updates to table '{}.{}.{}'", database_name, schema_name, table_name)))?;
    
    Ok(Json(CommitTableResponse::new(
        updated_table_rwo.data.metadata_location,
        updated_table_rwo.data.metadata,
    )))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_table(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> IcebergAPIResult<Json<LoadTableResult>> {
    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_name);
    
    let table_rwo_opt = state
        .metastore
        .get_table(&table_ident)
        .await
        .map_err(|e| map_metastore_error(e, format!("API: Failed to query table '{}.{}.{}'", database_name, schema_name, table_name)))?;

    let table_rwo = table_rwo_opt.ok_or_else(|| {
        let kind = MetastoreErrorKind::TableNotFound { db: database_name.clone(), schema: schema_name.clone(), table: table_name.clone() };
        let metastore_err = cm_error::OperationFailedSnafu { source: wrap_error(kind, "Table does not exist in metastore".to_string()) }.build();
        map_metastore_error(metastore_err, format!("API: Table '{}.{}.{}' not found", database_name, schema_name, table_name))
    })?;
    
    Ok(Json(LoadTableResult::new(table_rwo.data.metadata)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_table(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> IcebergAPIResult<StatusCode> {
    let table_ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_name);
    state
        .metastore
        .delete_table(&table_ident, true) // cascade delete
        .await
        .map_err(|e| map_metastore_error(e, format!("API: Failed to delete table '{}.{}.{}'", database_name, schema_name, table_name)))?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_tables(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> IcebergAPIResult<Json<ListTablesResponse>> {
    let schema_ident = MetastoreSchemaIdent::new(database_name.clone(), schema_name.clone());
    
    let tables_rwos = state
        .metastore
        .iter_tables(&schema_ident)
        .map_err(|e| map_metastore_error(e, format!("API: Failed to list tables in namespace '{}.{}'", database_name, schema_name)))?
        .collect::<cm_error::MetastoreResult<Vec<_>>>()
        .await
        .map_err(|e| map_metastore_error(e, format!("API: Failed to collect tables from iterator for '{}.{}'", database_name, schema_name)))?;
        
    let tables_data = tables_rwos.into_iter().map(|rwo| rwo.data).collect();
    Ok(Json(from_tables_list(tables_data)))
}

#[tracing::instrument(level = "debug", skip(_state), err, ret(level = tracing::Level::TRACE))]
pub async fn report_metrics(
    State(_state): State<AppState>, // Metrics are not processed by this stub
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
    Json(metrics): Json<Value>, // Metrics payload is Value, can be anything
) -> IcebergAPIResult<StatusCode> {
    tracing::info!(
        "Received metrics for table {}.{}.{}: {:?}",
        database_name, schema_name, table_name, metrics
    );
    // In a real implementation, this would process/store the metrics.
    // If processing failed, it might return an error.
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_config(
    State(state): State<AppState>,
    Query(params): Query<GetConfigQuery>, // warehouse (prefix) is in query params
) -> IcebergAPIResult<Json<CatalogConfig>> {
    let catalog_url = state.config.iceberg_catalog_url.clone();
    let prefix = params.warehouse.unwrap_or_else(|| "default_warehouse".to_string()); // Use a default if not provided

    // These properties are examples. Real properties would depend on catalog requirements.
    let mut defaults = HashMap::new();
    defaults.insert("client.assume-role.region".to_string(), "us-east-1".to_string());

    let mut overrides = HashMap::new();
    overrides.insert("uri".to_string(), format!("{}/v1/{}", catalog_url, prefix)); // Points back to this catalog but with warehouse prefix
    overrides.insert("prefix".to_string(), prefix.clone()); // The warehouse prefix itself

    let config = CatalogConfig {
        defaults,
        overrides,
        endpoints: None, // No separate S3/token endpoints specified here
    };
    Ok(Json(config))
}

#[tracing::instrument(level = "debug", skip(_state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_views(
    State(_state): State<AppState>, // Views are not implemented in this metastore
    Path((_database_name, _schema_name)): Path<(String, String)>, // Namespace for views
) -> IcebergAPIResult<Json<ListTablesResponse>> { // ListTablesResponse is also used for views
    // Since views are not supported, return an empty list.
    // Alternatively, could return a 501 Not Implemented if the spec allows.
    // Iceberg REST spec typically expects an empty list if no views exist or not supported.
    Ok(Json(ListTablesResponse {
        next_page_token: None,
        identifiers: Some(vec![]), // Explicitly empty vec
    }))
}
