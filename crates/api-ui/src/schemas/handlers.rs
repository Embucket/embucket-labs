use crate::schemas::models::SchemasParameters;
use crate::state::AppState;
use crate::{
    error::ErrorResponse,
    schemas::error::{SchemasAPIError, SchemasResult},
    schemas::models::{
        Schema, SchemaCreatePayload, SchemaCreateResponse, SchemaResponse, SchemaUpdatePayload,
        SchemaUpdateResponse, SchemasResponse,
    },
};
use api_sessions::DFSessionId;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use core_executor::query::QueryContext;
use core_metastore::error::MetastoreError;
use core_metastore::models::SchemaIdent as MetastoreSchemaIdent;
use core_utils::scan_iterator::ScanIterator;
use std::convert::From;
use std::convert::Into;
use chrono::NaiveDateTime;
use datafusion::arrow::util::display::array_value_to_string;
use utoipa::OpenApi;
use core_executor::models::QueryResultData;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_schema,
        delete_schema,
        // update_schema,
        // get_schema,
        list_schemas,
    ),
    components(
        schemas(
            SchemaCreatePayload,
            SchemaCreateResponse,
            SchemasResponse,
            Schema,
            ErrorResponse,
        )
    ),
    tags(
        (name = "schemas", description = "Schemas endpoints")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    path = "/ui/databases/{databaseName}/schemas",
    operation_id = "createSchema",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name")
    ),
    request_body = SchemaCreatePayload,
    responses(
        (status = 200, description = "Successful Response", body = SchemaCreateResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_schema(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(payload): Json<SchemaCreatePayload>,
) -> SchemasResult<Json<SchemaCreateResponse>> {
    let context = QueryContext::new(
        Some(database_name.clone()),
        Some(payload.name.clone()),
        None,
    );
    let sql_string = format!(
        "CREATE SCHEMA {}.{}",
        database_name.clone(),
        payload.name.clone()
    );
    let _ = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .map_err(|e| SchemasAPIError::Create { source: e })?;
    Ok(Json(SchemaCreateResponse {
        data: Schema::new(payload.name, database_name),
    }))
}

#[utoipa::path(
    delete,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}",
    operation_id = "deleteSchema",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name")
    ),
    responses(
        (status = 204, description = "Successful Response"),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Schema not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_schema(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> SchemasResult<()> {
    let context = QueryContext::new(Some(database_name.clone()), Some(schema_name.clone()), None);
    let sql_string = format!(
        "DROP SCHEMA {}.{}",
        database_name.clone(),
        schema_name.clone()
    );
    let _ = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .map_err(|e| SchemasAPIError::Delete { source: e })?;
    Ok(())
}

#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}",
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name")
    ),
    operation_id = "getSchema",
    tags = ["schemas"],
    responses(
        (status = 200, description = "Successful Response", body = SchemaResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Schema not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> SchemasResult<Json<SchemaResponse>> {
    let schema_ident = MetastoreSchemaIdent {
        database: database_name.clone(),
        schema: schema_name.clone(),
    };
    match state.metastore.get_schema(&schema_ident).await {
        Ok(Some(rw_object)) => Ok(Json(SchemaResponse {
            data: Schema::from(rw_object),
        })),
        Ok(None) => Err(SchemasAPIError::Get {
            source: MetastoreError::SchemaNotFound {
                db: database_name.clone(),
                schema: schema_name.clone(),
            },
        }),
        Err(e) => Err(SchemasAPIError::Get { source: e }),
    }
}

#[utoipa::path(
    put,
    operation_id = "updateSchema",
    path="/ui/databases/{databaseName}/schemas/{schemaName}",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name")
    ),
    request_body = SchemaUpdatePayload,
    responses(
        (status = 200, body = SchemaUpdateResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Schema not found"),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(schema): Json<SchemaUpdatePayload>,
) -> SchemasResult<Json<SchemaUpdateResponse>> {
    let schema_ident = MetastoreSchemaIdent::new(database_name, schema_name);
    // TODO: Implement schema renames
    state
        .metastore
        .update_schema(&schema_ident, schema.data.into())
        .await
        .map_err(|e| SchemasAPIError::Update { source: e })
        .map(|rw_object| {
            Json(SchemaUpdateResponse {
                data: Schema::from(rw_object),
            })
        })
}

#[utoipa::path(
    get,
    operation_id = "getSchemas",
    path="/ui/databases/{databaseName}/schemas",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name"),
        ("offset" = Option<usize>, Query, description = "Schemas offset"),
        ("limit" = Option<u16>, Query, description = "Schemas limit"),
        ("search" = Option<String>, Query, description = "Schemas search"),
        ("order_by" = Option<String>, Query, description = "Order by: schema_name (default), database_name, created_at, updated_at"),
        ("order_direction" = Option<String>, Query, description = "Order direction: ASC (default), DESC"),
    ),
    responses(
        (status = 200, body = SchemasResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_schemas(
    DFSessionId(session_id): DFSessionId,   
    Query(parameters): Query<SchemasParameters>,
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> SchemasResult<Json<SchemasResponse>> {
    let context = QueryContext::new(
        Some(database_name.clone()),
        None,
        None,
    );
    let sql_string = format!(
        "SELECT * FROM slatedb.public.schemas WHERE database_name = '{}'",
        database_name.clone()
    );
    let sql_string = parameters.search.map_or(sql_string.clone(), |search| 
        format!("{sql_string} AND (schema_name LIKE '%{search}%' OR database_name LIKE '%{search}%' OR created_at LIKE '%{search}%' OR updated_at LIKE '%{search}%')")
    );
    let sql_string = parameters.order_by.map_or(format!("{sql_string} ORDER BY schema_name"), |order_by| format!("{sql_string} ORDER BY {order_by}"));
    let sql_string = parameters.order_direction.map_or(format!("{sql_string} DESC"), |order_direction| format!("{sql_string} {order_direction}"));
    let sql_string = parameters.offset.map_or(sql_string.clone(), |offset| format!("{sql_string} OFFSET {offset}"));
    let sql_string = parameters.limit.map_or(sql_string.clone(), |limit| format!("{sql_string} LIMIT {limit}"));
    let QueryResultData { records, .. } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .map_err(|e| SchemasAPIError::List { source: e })?;
    let mut items = Vec::new();
    for record in records {
        let schema_names = record.column_by_name("schema_name").unwrap().as_ref();
        let database_names = record.column_by_name("database_name").unwrap().as_ref();
        let created_at_timestamps = record.column_by_name("created_at").unwrap().as_ref();
        let updated_at_timestamps = record.column_by_name("updated_at").unwrap().as_ref();
        for i in 0..record.num_rows() {
            items.push(Schema {
                name: array_value_to_string(schema_names, i).unwrap_or("ERROR".to_string()),
                database: array_value_to_string(database_names, i).unwrap_or("ERROR".to_string()),
                created_at: array_value_to_string(created_at_timestamps, i).unwrap_or("ERROR".to_string()),
                updated_at: array_value_to_string(updated_at_timestamps, i).unwrap_or("ERROR".to_string()),
            })
        }
    }
    Ok(Json(SchemasResponse {
        items,
    }))
}
