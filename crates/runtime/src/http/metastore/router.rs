use crate::http::state::AppState;
use axum::routing::{delete, get, post, put};
use axum::Router;

#[allow(clippy::wildcard_imports)]
use crate::http::metastore::handlers::*;

pub fn create_router() -> Router<AppState> {
    let metastore_router = Router::new()
        .route("/volumes", get(list_volumes))
        .route("/volumes", post(create_volume))
        .route("/volumes/{volumeName}", get(get_volume))
        .route("/volumes/{volumeName}", put(update_volume))
        .route("/volumes/{volumeName}", delete(delete_volume))
        .route("/databases", get(list_databases))
        .route("/databases", post(create_database))
        .route("/databases/{databaseName}", get(get_database))
        .route("/databases/{databaseName}", put(update_database))
        .route("/databases/{databaseName}", delete(delete_database))
        .route("/databases/{databaseName}/schemas", get(list_schemas))
        .route("/databases/{databaseName}/schemas", post(create_schema))
        .route(
            "/databases/{databaseName}/schemas/{schemaName}",
            get(get_schema),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}",
            put(update_schema),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}",
            delete(delete_schema),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables",
            get(list_tables),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables",
            post(create_table),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}",
            get(get_table),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}",
            put(update_table),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}",
            delete(delete_table),
        );

    Router::new().nest("/v1/metastore", metastore_router)
}
