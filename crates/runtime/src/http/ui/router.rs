// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::http::layers::add_request_metadata;
use crate::http::ui::databases::handlers::{
    create_database, delete_database, get_database, list_databases, update_database,
};
use crate::http::ui::schemas::handlers::{create_schema, delete_schema, list_schemas};
use crate::http::ui::volumes::handlers::{
    create_volume, delete_volume, get_volume, list_volumes, update_volume,
};
use crate::http::ui::worksheets::handlers::{
    create_worksheet, delete_worksheet, update_worksheet, worksheet, worksheets,
};

use crate::http::ui::queries::handlers::{queries, query};
// use crate::http::ui::old_handlers::tables::{
//     create_table, delete_table, get_settings, get_snapshots, get_table, register_table,
//     update_table_properties, upload_data_to_table,
// };

use crate::http::ui::databases::handlers::ApiDoc as DatabasesApiDoc;
use crate::http::ui::navigation_trees::handlers::{
    get_navigation_trees, ApiDoc as DatabasesNavigationApiDoc,
};
use crate::http::ui::queries::handlers::ApiDoc as QueryApiDoc;
use crate::http::ui::schemas::handlers::ApiDoc as SchemasApiDoc;
use crate::http::ui::tables::handlers::{
    get_table_info, get_table_preview_data, ApiDoc as TableApiDoc,
};
use crate::http::ui::volumes::handlers::ApiDoc as VolumesApiDoc;
use crate::http::ui::worksheets::handlers::ApiDoc as WorksheetsApiDoc;

use crate::http::state::AppState;
use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, get, post};
use axum::Router;
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
}

pub fn create_router() -> Router<AppState> {
    Router::new()
        // .route("/navigation_trees", get(navigation_trees))
        .route("/navigation-trees", get(get_navigation_trees))
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
        // .route(
        //     "/warehouses/{warehouseId}/databases/{databaseName}/register",
        //     post(register_table),
        // )
        // .route(
        //     "/warehouses/{warehouseId}/databases/{databaseName}/tables",
        //     post(create_table),
        // )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/info",
            get(get_table_info),
        )
        .route(
            "/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/preview",
            get(get_table_preview_data),
        )
        .route("/worksheets", get(worksheets).post(create_worksheet))
        .route(
            "/worksheets/{worksheet_id}",
            get(worksheet)
                .delete(delete_worksheet)
                .patch(update_worksheet),
        )
        .route("/queries", post(query).get(queries))
        // .route(
        //     "/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}/settings",
        //     get(get_settings).post(update_table_properties),
        // )
        // .route(
        //     "/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}/upload",
        //     post(upload_data_to_table),
        // )
        // .route(
        //     "/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}/snapshots",
        //     get(get_snapshots),
        // )
        .route("/volumes", post(create_volume).get(list_volumes))
        .route(
            "/volumes/{volumeName}",
            delete(delete_volume).get(get_volume).put(update_volume),
        )
        .layer(SetSensitiveHeadersLayer::new([
            axum::http::header::AUTHORIZATION,
        ]))
        .layer(axum::middleware::from_fn(add_request_metadata))
        .layer(DefaultBodyLimit::disable())
}
