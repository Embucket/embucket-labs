#![allow(clippy::needless_for_each)]
use crate::error::Result;
use crate::state::AppState;
use crate::{OrderDirection, apply_parameters};
use crate::{
    SearchParameters,
    databases::error::{
        self as databases_error, CreateQuerySnafu, CreateSnafu, DatabaseNotFoundSnafu, GetSnafu,
        UpdateSnafu,
    },
    databases::models::{
        Database, DatabaseCreatePayload, DatabaseCreateResponse, DatabaseResponse,
        DatabaseUpdatePayload, DatabaseUpdateResponse, DatabasesResponse,
    },
    downcast_string_column,
    error::ErrorResponse,
};
use api_sessions::DFSessionId;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use core_executor::models::{QueryContext, QueryResult};
use core_metastore::Database as MetastoreDatabase;
use core_metastore::error::{self as metastore_error, ValidationSnafu};
use snafu::{OptionExt, ResultExt};
use utoipa::OpenApi;
use validator::Validate;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_database,
        get_database,
        delete_database,
        list_databases,
        // update_database,
    ),
    components(
        schemas(
            DatabaseCreatePayload,
            DatabaseCreateResponse,
            DatabaseResponse,
            DatabasesResponse,
            //DatabasePayload,
            Database,
            ErrorResponse,
            OrderDirection,
        )
    ),
    tags(
        (name = "databases", description = "Databases endpoints")
    )
)]
pub struct ApiDoc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryParameters {
    #[serde(default)]
    pub cascade: Option<bool>,
}

#[utoipa::path(
    post,
    operation_id = "createDatabase",
    tags = ["databases"],
    path = "/ui/databases",
    request_body = DatabaseCreatePayload,
    responses(
        (status = 200, description = "Successful Response", body = DatabaseCreateResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 409, description = "Already Exists", body = ErrorResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::create_database", level = "info", skip(state, database), err, ret(level = tracing::Level::TRACE))]
pub async fn create_database(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Json(database): Json<DatabaseCreatePayload>,
) -> Result<Json<DatabaseCreateResponse>> {
    let database = MetastoreDatabase {
        ident: database.name,
        volume: database.volume,
        properties: None,
    };
    database
        .validate()
        .context(ValidationSnafu)
        .context(CreateSnafu)?;
    state
        .execution_svc
        .query(
            &session_id,
            &format!(
                "CREATE DATABASE {} EXTERNAL_VOLUME = '{}'",
                database.ident, database.volume
            ),
            QueryContext::default(),
        )
        .await
        .context(CreateQuerySnafu)?;

    let database = state
        .metastore
        .get_database(&database.ident)
        .await
        .context(GetSnafu)?
        .context(DatabaseNotFoundSnafu {
            database: database.ident.clone(),
        })?;

    Ok(Json(DatabaseCreateResponse(Database::from(database))))
}

#[utoipa::path(
    get,
    operation_id = "getDatabase",
    tags = ["databases"],
    path = "/ui/databases/{databaseName}",
    params(
        ("databaseName" = String, Path, description = "Database name")
    ),
    responses(
        (status = 200, description = "Successful Response", body = DatabaseResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::get_database", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> Result<Json<DatabaseResponse>> {
    let database = state
        .metastore
        .get_database(&database_name)
        .await
        .map(|opt_rw_obj| {
            opt_rw_obj.ok_or_else(|| {
                metastore_error::DatabaseNotFoundSnafu {
                    db: database_name.clone(),
                }
                .build()
            })
        })
        .context(GetSnafu)?
        .map(Database::from)
        .context(GetSnafu)?;

    Ok(Json(DatabaseResponse(database)))
}

#[utoipa::path(
    delete,
    operation_id = "deleteDatabase",
    tags = ["databases"],
    path = "/ui/databases/{databaseName}",
    params(
        ("databaseName" = String, Path, description = "Database name")
    ),
    responses(
        (status = 200, description = "Successful Response"),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
    )
)]
pub async fn delete_database(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(database_name): Path<String>,
) -> Result<()> {
    let cascade = if query.cascade.unwrap_or_default() {
        " CASCADE"
    } else {
        ""
    };
    state
        .execution_svc
        .query(
            &session_id,
            &format!("DROP DATABASE {database_name}{cascade}"),
            QueryContext::default(),
        )
        .await
        .context(crate::schemas::error::DeleteSnafu)?;
    Ok(())
}

#[utoipa::path(
    put,
    operation_id = "updateDatabase",
    tags = ["databases"],
    path = "/ui/databases/{databaseName}",
    request_body = DatabaseUpdatePayload,
    params(
        ("databaseName" = String, Path, description = "Database name")
    ),
    responses(
        (status = 200, description = "Successful Response", body = DatabaseUpdateResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 400, description = "Invalid data", body = ErrorResponse)
    )
)]
#[tracing::instrument(name = "api_ui::update_database", level = "info", skip(state, database), err, ret(level = tracing::Level::TRACE))]
pub async fn update_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(database): Json<DatabaseUpdatePayload>,
) -> Result<Json<DatabaseUpdateResponse>> {
    let database = MetastoreDatabase {
        ident: database.name,
        volume: database.volume,
        properties: None,
    };
    database
        .validate()
        .context(ValidationSnafu)
        .context(UpdateSnafu)?;
    //TODO: Implement database renames
    let database = state
        .metastore
        .update_database(&database_name, database)
        .await
        .map(Database::from)
        .context(UpdateSnafu)?;

    Ok(Json(DatabaseUpdateResponse(database)))
}

#[utoipa::path(
    get,
    operation_id = "getDatabases",
    params(
        ("offset" = Option<usize>, Query, description = "Databases offset"),
        ("limit" = Option<usize>, Query, description = "Databases limit"),
        ("search" = Option<String>, Query, description = "Databases search"),
        ("order_by" = Option<String>, Query, description = "Order by: database_name, volume_name, created_at (default), updated_at"),
        ("order_direction" = Option<OrderDirection>, Query, description = "Order direction: ASC, DESC (default)"),
    ),
    tags = ["databases"],
    path = "/ui/databases",
    responses(
        (status = 200, body = DatabasesResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(name = "api_ui::list_databases", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn list_databases(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<SearchParameters>,
    State(state): State<AppState>,
) -> Result<Json<DatabasesResponse>> {
    let context = QueryContext::default();
    let sql_string = "SELECT * FROM slatedb.meta.databases".to_string();
    let sql_string = apply_parameters(
        &sql_string,
        parameters,
        &["database_name", "volume_name"],
        "created_at",
        OrderDirection::DESC,
    );
    let QueryResult { records, .. } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .context(databases_error::ListSnafu)?;
    let mut items = Vec::new();
    for record in records {
        let database_names =
            downcast_string_column(&record, "database_name").context(databases_error::ListSnafu)?;
        let volume_names =
            downcast_string_column(&record, "volume_name").context(databases_error::ListSnafu)?;
        let created_at_timestamps =
            downcast_string_column(&record, "created_at").context(databases_error::ListSnafu)?;
        let updated_at_timestamps =
            downcast_string_column(&record, "updated_at").context(databases_error::ListSnafu)?;
        for i in 0..record.num_rows() {
            items.push(Database {
                name: database_names.value(i).to_string(),
                volume: volume_names.value(i).to_string(),
                created_at: created_at_timestamps.value(i).to_string(),
                updated_at: updated_at_timestamps.value(i).to_string(),
            });
        }
    }
    Ok(Json(DatabasesResponse { items }))
}
