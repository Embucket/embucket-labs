#![allow(clippy::needless_for_each)]
use crate::OrderDirection;
use crate::Result;
use crate::state::AppState;
use crate::{
    SearchParameters,
    error::ErrorResponse,
    schemas::error::{CreateSnafu, DeleteSnafu, GetSnafu, ListSnafu, UpdateSnafu},
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
use core_executor::models::QueryContext;
use core_metastore::error as metastore_error;
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
        ("databaseName" = String, Path, description = "Database Name")
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
#[tracing::instrument(name = "api_ui::create_schema", level = "info", skip(state, payload), err, ret(level = tracing::Level::TRACE))]
pub async fn create_schema(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(payload): Json<SchemaCreatePayload>,
) -> Result<Json<SchemaCreateResponse>> {
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
    let schema = state
        .metastore
        .get_schema(&schema_ident)
        .await
        .map(|opt_rw_obj| {
            // Here we create core_metastore::Error since Metastore instead of error returns Option = None
            // TODO: Remove after refactor Metastore
            opt_rw_obj
                .ok_or_else(|| {
                    metastore_error::SchemaNotFoundSnafu {
                        db: database_name.clone(),
                        schema: payload.name.clone(),
                    }
                    .build()
                })
                .context(GetSnafu)
        })
        .context(GetSnafu)?
        .map(Schema::try_from)??;
    Ok(Json(SchemaCreateResponse(schema)))
}

#[utoipa::path(
    delete,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}",
    operation_id = "deleteSchema",
    tags = ["schemas"],
    params(
        ("databaseName" = String, Path, description = "Database Name"),
        ("schemaName" = String, Path, description = "Schema Name")
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
#[tracing::instrument(name = "api_ui::delete_schema", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_schema(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> Result<()> {
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
        .context(DeleteSnafu)?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}",
    params(
        ("databaseName" = String, Path, description = "Database Name"),
        ("schemaName" = String, Path, description = "Schema Name")
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
#[tracing::instrument(name = "api_ui::get_schema", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> Result<Json<SchemaResponse>> {
    let schema_ident = MetastoreSchemaIdent {
        database: database_name.clone(),
        schema: schema_name.clone(),
    };
    let schema = state
        .metastore
        .get_schema(&schema_ident)
        .await
        .map(|opt_rw_obj| {
            // We create here core_metastore::Error since Metastore instead of error returns Option = None
            // TODO: Remove after refactor Metastore
            opt_rw_obj
                .ok_or_else(|| {
                    metastore_error::SchemaNotFoundSnafu {
                        db: database_name.clone(),
                        schema: schema_name.clone(),
                    }
                    .build()
                })
                .context(GetSnafu)
        })
        .context(GetSnafu)??;

    Ok(Json(SchemaResponse(Schema::try_from(schema)?)))
}

#[utoipa::path(
    put,
    operation_id = "updateSchema",
    path="/ui/databases/{databaseName}/schemas/{schemaName}",
    tags = ["schemas"],
    params(
        ("databaseName" = String, Path, description = "Database Name"),
        ("schemaName" = String, Path, description = "Schema Name")
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
#[tracing::instrument(name = "api_ui::update_schema", level = "info", skip(state, schema), err, ret(level = tracing::Level::TRACE))]
pub async fn update_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(schema): Json<SchemaUpdatePayload>,
) -> Result<Json<SchemaUpdateResponse>> {
    let schema_ident = MetastoreSchemaIdent::new(schema.database, schema.name);
    let metastore_schema = MetastoreSchema {
        ident: schema_ident.clone(),
        properties: None,
    };
    // TODO: Implement schema renames
    let schema = state
        .metastore
        .update_schema(&schema_ident, metastore_schema)
        .await
        .context(UpdateSnafu)?;

    Ok(Json(SchemaUpdateResponse(Schema::try_from(schema)?)))
}

#[utoipa::path(
    get,
    operation_id = "getSchemas",
    path="/ui/databases/{databaseName}/schemas",
    tags = ["schemas"],
    params(
        ("databaseName" = String, Path, description = "Database Name"),
        ("offset" = Option<usize>, Query, description = "Schemas offset"),
        ("limit" = Option<u16>, Query, description = "Schemas limit"),
        ("search" = Option<String>, Query, description = "Schemas search"),
        ("orderBy" = Option<String>, Query, description = "Order by: schema_name, database_name, created_at (default), updated_at"),
        ("orderDirection" = Option<OrderDirection>, Query, description = "Order direction: ASC, DESC (default)"),
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
#[tracing::instrument(name = "api_ui::list_schemas", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn list_schemas(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<SearchParameters>,
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> Result<Json<SchemasResponse>> {
    let items = state
        .metastore
        .get_schemas(parameters.into())
        .await
        .context(ListSnafu)?
        .into_iter()
        .map(Schema::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(Json(SchemasResponse { items }))
}
