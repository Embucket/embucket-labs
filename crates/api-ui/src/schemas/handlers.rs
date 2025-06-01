use crate::state::AppState;
use crate::{OrderDirection, apply_parameters};
use crate::{
    SearchParameters, downcast_string_column,
    error::ErrorResponse,
    schemas::error::{CreateSnafu, DeleteSnafu, GetSnafu, ListSnafu, SchemasResult, UpdateSnafu},
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
use core_executor::models::{QueryContext, QueryResult};
use core_metastore::error::MetastoreError;
use core_metastore::models::{Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent};
use snafu::ResultExt;
use std::convert::From;
use std::convert::Into;
use utoipa::OpenApi;

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
            // SchemaPayload,
            Schema,
            ErrorResponse,
            OrderDirection,
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
        .context(CreateSnafu)?;

    let schema_ident = MetastoreSchemaIdent::new(database_name.clone(), payload.name.clone());
    // after created - request schema from metadata for timestamps
    state
        .metastore
        .get_schema(&schema_ident)
        .await
        .map(|opt_rw_obj| {
            opt_rw_obj.ok_or_else(|| {
                Box::new(MetastoreError::SchemaNotFound {
                    db: database_name.clone(),
                    schema: payload.name.clone(),
                })
            })
        })
        .context(GetSnafu)?
        .map(Schema::from)
        .map(SchemaCreateResponse)
        .map(Json)
        .context(GetSnafu)
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
    state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .map(|_| ())
        .context(DeleteSnafu)
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
    state
        .metastore
        .get_schema(&schema_ident)
        .await
        .map(|opt_rw_obj| {
            opt_rw_obj.ok_or_else(|| {
                Box::new(MetastoreError::SchemaNotFound {
                    db: database_name.clone(),
                    schema: schema_name.clone(),
                })
            })
        })
        .context(GetSnafu)?
        .map(Schema::from)
        .map(SchemaResponse)
        .map(Json)
        .context(GetSnafu)
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
    let schema_ident = MetastoreSchemaIdent::new(schema.database, schema.name);
    let metastore_schema = MetastoreSchema {
        ident: schema_ident.clone(),
        properties: None,
    };
    // TODO: Implement schema renames
    state
        .metastore
        .update_schema(&schema_ident, metastore_schema)
        .await
        .map(Schema::from)
        .map(SchemaUpdateResponse)
        .map(Json)
        .context(UpdateSnafu)
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
        ("order_direction" = Option<OrderDirection>, Query, description = "Order direction: ASC, DESC (default)"),
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
#[allow(clippy::unwrap_used)]
pub async fn list_schemas(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<SearchParameters>,
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> SchemasResult<Json<SchemasResponse>> {
    let context = QueryContext::new(Some(database_name.clone()), None, None);
    let sql_string = format!(
        "SELECT * FROM slatedb.meta.schemas WHERE database_name = '{}'",
        database_name.clone()
    );
    let sql_string = apply_parameters(&sql_string, parameters, &["schema_name", "database_name"]);
    let QueryResult { records, .. } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .context(ListSnafu)?;

    let mut items = Vec::new();
    for record in records {
        let schema_names = downcast_string_column(&record, "schema_name").context(ListSnafu)?;
        let database_names = downcast_string_column(&record, "database_name").context(ListSnafu)?;
        let created_at_timestamps =
            downcast_string_column(&record, "created_at").context(ListSnafu)?;
        let updated_at_timestamps =
            downcast_string_column(&record, "updated_at").context(ListSnafu)?;
        for i in 0..record.num_rows() {
            items.push(Schema {
                name: schema_names.value(i).to_string(),
                database: database_names.value(i).to_string(),
                created_at: created_at_timestamps.value(i).to_string(),
                updated_at: updated_at_timestamps.value(i).to_string(),
            });
        }
    }
    Ok(Json(SchemasResponse { items }))
}
