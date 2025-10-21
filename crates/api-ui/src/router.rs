#![allow(clippy::needless_for_each)]
use crate::databases::handlers::ApiDoc as DatabasesApiDoc;
use crate::databases::handlers::{
    create_database, delete_database, get_database, list_databases, update_database,
};
use crate::layers::add_request_metadata;
use crate::navigation_trees::handlers::{
    ApiDoc as DatabasesNavigationApiDoc, get_navigation_trees,
};
use crate::queries::handlers::{ApiDoc as QueryApiDoc, get_query};
use crate::queries::handlers::{get_query_result, queries, query};
use crate::schemas::handlers::ApiDoc as SchemasApiDoc;
use crate::schemas::handlers::{create_schema, delete_schema, list_schemas};
use crate::tables::handlers::{
    ApiDoc as TableApiDoc, get_table_columns, get_table_preview_data, get_table_statistics,
    get_tables, upload_file,
};
use crate::volumes::handlers::ApiDoc as VolumesApiDoc;
use crate::volumes::handlers::{
    create_volume,
    delete_volume,
    get_volume,
    list_volumes, // update_volume,
};
use crate::worksheets::handlers::ApiDoc as WorksheetsApiDoc;
use crate::worksheets::handlers::{
    create_worksheet, delete_worksheet, update_worksheet, worksheet, worksheets,
};

use crate::auth::handlers::ApiDoc as AuthApiDoc;
use crate::dashboard::handlers::{ApiDoc as DashboardApiDoc, get_dashboard};
use crate::state::AppState;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, get, post};
use tower_http::sensitive_headers::SetSensitiveHeadersLayer;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "UI Router API",
        description = "API documentation for the UI endpoints.",
        version = "1.0.2",
        license(
            name = "Apache 2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0.html"
        ),
        contact(name = "Embucket, Inc.", url = "https://embucket.com"),
        description = "Defines the specification for the UI Catalog API",
    ),
    tags()
)]
pub struct ApiDoc;

#[must_use]
pub fn ui_open_api_spec() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
        .merge_from(VolumesApiDoc::openapi())
        .merge_from(DatabasesApiDoc::openapi())
        .merge_from(SchemasApiDoc::openapi())
        .merge_from(TableApiDoc::openapi())
        .merge_from(WorksheetsApiDoc::openapi())
        .merge_from(QueryApiDoc::openapi())
        .merge_from(DatabasesNavigationApiDoc::openapi())
        .merge_from(DashboardApiDoc::openapi())
        .merge_from(AuthApiDoc::openapi())
}

pub fn create_router() -> Router<AppState> {
    Router::new()
        // .route("/navigation_trees", get(navigation_trees))
        .route("/navigation-trees", get(get_navigation_trees))
        .route("/dashboard", get(get_dashboard))
        .route(
            "/databases/{databaseName}/schemas/{schemaName}",
            delete(delete_schema),
        )
        .route(
            "/databases/{databaseName}/schemas",
            post(create_schema).get(list_schemas),
        )
        .route("/databases", post(create_database).get(list_databases))
        .route(
            "/databases/{databaseName}",
            delete(delete_database)
                .get(get_database)
                .put(update_database),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables",
            get(get_tables),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/statistics",
            get(get_table_statistics),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/columns",
            get(get_table_columns),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/rows",
            get(get_table_preview_data),
        )
        .route("/worksheets", get(worksheets).post(create_worksheet))
        .route(
            "/worksheets/{worksheetId}",
            get(worksheet)
                .delete(delete_worksheet)
                .patch(update_worksheet),
        )
        .route("/queries", post(query).get(queries))
        .route("/queries/{queryRecordId}", get(get_query))
        .route("/queries/{queryRecordId}/result", get(get_query_result))
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/rows",
            post(upload_file),
        )
        .route("/volumes", post(create_volume).get(list_volumes))
        .route(
            "/volumes/{volumeName}",
            delete(delete_volume).get(get_volume), /* .put(update_volume) */
        )
        .layer(SetSensitiveHeadersLayer::new([
            axum::http::header::AUTHORIZATION,
        ]))
        .layer(axum::middleware::from_fn(add_request_metadata))
        .layer(DefaultBodyLimit::disable())
}
