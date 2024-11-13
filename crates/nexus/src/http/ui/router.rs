use crate::http::layers::add_request_metadata;
use crate::http::ui::handlers::databases::{create_database, delete_database, get_database};
use crate::http::ui::handlers::profiles::{
    create_storage_profile, delete_storage_profile, get_storage_profile, list_storage_profiles,
};
use crate::http::ui::handlers::tables::{
    create_table, delete_table, get_settings, get_snapshots, get_table, query_table,
    update_table_properties, upload_data_to_table,
};
use crate::http::ui::handlers::warehouses::{
    create_warehouse, delete_warehouse, get_warehouse, list_warehouses, navigation,
};
use crate::state::AppState;
use axum::routing::{delete, get, post};
use axum::Router;
use tower_http::sensitive_headers::SetSensitiveHeadersLayer;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(info(
    title = "UI Router API",
    description = "API documentation for the UI endpoints.",
    version = "1.0.0",
    license(
        name = "Apache 2.0",
        url = "https://www.apache.org/licenses/LICENSE-2.0.html"
    ),
    contact(name = "Embucket, Inc.", url = "https://embucket.com"),
    description = "Defines the specification for the UI Catalog API",
))]
pub struct ApiDoc;

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/navigation", get(navigation))
        .route("/warehouses", post(create_warehouse).get(list_warehouses))
        .route(
            "/warehouses/:warehouseId",
            delete(delete_warehouse).get(get_warehouse),
        )
        .route(
            "/warehouses/:warehouseId/databases/:databaseName",
            get(get_database).delete(delete_database),
        )
        .route("/warehouses/:warehouseId/databases", post(create_database))
        .route(
            "/warehouses/:warehouseId/databases/:databaseName/tables",
            post(create_table).delete(delete_table),
        )
        .route(
            "/warehouses/:warehouseId/databases/:databaseName/tables/:tableName",
            get(get_table),
        )
        .route(
            "/warehouses/:warehouseId/databases/:databaseName/tables/:tableName/query",
            post(query_table),
        )
        .route(
            "/warehouses/:warehouseId/databases/:databaseName/tables/:tableName/settings",
            get(get_settings).post(update_table_properties),
        )
        .route(
            "/warehouses/:warehouseId/databases/:databaseName/tables/:tableName/upload",
            post(upload_data_to_table),
        )
        .route(
            "/warehouses/:warehouseId/databases/:databaseName/tables/:tableName/snapshots",
            get(get_snapshots),
        )
        .route(
            "/storage-profiles",
            post(create_storage_profile).get(list_storage_profiles),
        )
        .route(
            "/storage-profiles/:storageProfileId",
            delete(delete_storage_profile).get(get_storage_profile),
        )
        .layer(SetSensitiveHeadersLayer::new([
            axum::http::header::AUTHORIZATION,
        ]))
        .layer(axum::middleware::from_fn(add_request_metadata))
}
