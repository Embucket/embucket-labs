use crate::databases::models::DatabasesParameters;
use crate::state::AppState;
use crate::{
    databases::error::{DatabasesAPIError, DatabasesResult},
    databases::models::{
        Database, DatabaseCreatePayload, DatabaseCreateResponse, DatabaseResponse,
        DatabaseUpdatePayload, DatabaseUpdateResponse, DatabasesResponse,
    },
    error::ErrorResponse,
};
use api_sessions::DFSessionId;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use core_executor::models::QueryResultData;
use core_executor::query::QueryContext;
use core_metastore::Database as MetastoreDatabase;
use core_metastore::error::MetastoreError;
use datafusion::arrow::util::display::array_value_to_string;
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
            Database,
            ErrorResponse,
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
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_database(
    State(state): State<AppState>,
    Json(database): Json<DatabaseCreatePayload>,
) -> DatabasesResult<Json<DatabaseCreateResponse>> {
    let database: MetastoreDatabase = database.data.into();
    database.validate().map_err(|e| DatabasesAPIError::Create {
        source: MetastoreError::Validation { source: e },
    })?;
    state
        .metastore
        .create_database(&database.ident.clone(), database)
        .await
        .map_err(|e| DatabasesAPIError::Create { source: e })
        .map(|o| Json(DatabaseCreateResponse { data: o.into() }))
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
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> DatabasesResult<Json<DatabaseResponse>> {
    match state.metastore.get_database(&database_name).await {
        Ok(Some(db)) => Ok(Json(DatabaseResponse { data: db.into() })),
        Ok(None) => Err(DatabasesAPIError::Get {
            source: MetastoreError::DatabaseNotFound {
                db: database_name.clone(),
            },
        }),
        Err(e) => Err(DatabasesAPIError::Get { source: e }),
    }
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
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_database(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(database_name): Path<String>,
) -> DatabasesResult<()> {
    state
        .metastore
        .delete_database(&database_name, query.cascade.unwrap_or_default())
        .await
        .map_err(|e| DatabasesAPIError::Delete { source: e })
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
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(database): Json<DatabaseUpdatePayload>,
) -> DatabasesResult<Json<DatabaseUpdateResponse>> {
    let database: MetastoreDatabase = database.data.into();
    database.validate().map_err(|e| DatabasesAPIError::Update {
        source: MetastoreError::Validation { source: e },
    })?;
    //TODO: Implement database renames
    state
        .metastore
        .update_database(&database_name, database)
        .await
        .map_err(|e| DatabasesAPIError::Update { source: e })
        .map(|o| Json(DatabaseUpdateResponse { data: o.into() }))
}

#[utoipa::path(
    get,
    operation_id = "getDatabases",
    params(
        ("offset" = Option<usize>, Query, description = "Databases offset"),
        ("limit" = Option<usize>, Query, description = "Databases limit"),
        ("search" = Option<String>, Query, description = "Databases search"),
        ("order_by" = Option<String>, Query, description = "Order by: database_name (default), volume_name, created_at, updated_at"),
        ("order_direction" = Option<String>, Query, description = "Order direction: ASC, DESC (default)"),
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
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn list_databases(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<DatabasesParameters>,
    State(state): State<AppState>,
) -> DatabasesResult<Json<DatabasesResponse>> {
    let context = QueryContext::new(None, None, None);
    let sql_string = "SELECT * FROM slatedb.public.databases".to_string();
    let sql_string = parameters.search.map_or_else(|| sql_string.clone(), |search|
        format!("{sql_string} WHERE (database_name LIKE '%{search}%' OR volume_name LIKE '%{search}%' OR created_at LIKE '%{search}%' OR updated_at LIKE '%{search}%')")
    );
    let sql_string = parameters.order_by.map_or_else(
        || format!("{sql_string} ORDER BY database_name"),
        |order_by| format!("{sql_string} ORDER BY {order_by}"),
    );
    let sql_string = parameters.order_direction.map_or_else(
        || format!("{sql_string} DESC"),
        |order_direction| format!("{sql_string} {order_direction}"),
    );
    let sql_string = parameters.offset.map_or_else(
        || sql_string.clone(),
        |offset| format!("{sql_string} OFFSET {offset}"),
    );
    let sql_string = parameters.limit.map_or_else(
        || sql_string.clone(),
        |limit| format!("{sql_string} LIMIT {limit}"),
    );
    let QueryResultData { records, .. } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .map_err(|e| DatabasesAPIError::List { source: e })?;
    let mut items = Vec::new();
    for record in records {
        let database_names = record.column_by_name("database_name").unwrap().as_ref();
        let volume_names = record.column_by_name("volume_name").unwrap().as_ref();
        let created_at_timestamps = record.column_by_name("created_at").unwrap().as_ref();
        let updated_at_timestamps = record.column_by_name("updated_at").unwrap().as_ref();
        for i in 0..record.num_rows() {
            items.push(Database {
                name: array_value_to_string(database_names, i).unwrap(),
                volume: array_value_to_string(volume_names, i).unwrap(),
                created_at: array_value_to_string(created_at_timestamps, i).unwrap(),
                updated_at: array_value_to_string(updated_at_timestamps, i).unwrap(),
            });
        }
    }
    Ok(Json(DatabasesResponse { items }))
}
