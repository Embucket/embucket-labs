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
// use crate::http::ui::handlers::databases::{create_database, delete_database, get_database};

use crate::http::ui::handlers::query::query;
use crate::http::ui::handlers::volumes::{
    create_volume, delete_volume, get_volume, list_volumes, update_volume,
};
// use crate::http::ui::handlers::tables::{
//     create_table, delete_table, get_settings, get_snapshots, get_table, register_table,
//     update_table_properties, upload_data_to_table,
// };
// use crate::http::ui::handlers::warehouses::{
//     create_warehouse, delete_warehouse, get_warehouse, list_warehouses, navigation,
// };
use crate::http::state::AppState;
use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, post};
use axum::Router;
use tower_http::sensitive_headers::SetSensitiveHeadersLayer;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(info(
    title = "UI Router API",
    description = "API documentation for the UI endpoints.",
    version = "1.0.2",
    license(
        name = "Apache 2.0",
        url = "https://www.apache.org/licenses/LICENSE-2.0.html"
    ),
    contact(name = "Embucket, Inc.", url = "https://embucket.com"),
    description = "Defines the specification for the UI Catalog API",
), tags(
    (name = "volumes", description = "Volumes endpoints"),
    (name = "databases", description = "Databases endpoints"),
    (name = "schemas", description = "Schemas endpoints"),
))]
pub struct ApiDoc;

pub fn create_router() -> Router<AppState> {
    Router::new()
        // .route("/navigation", get(navigation))
        // .route("/warehouses", post(create_warehouse).get(list_warehouses))
        // .route(
        //     "/warehouses/{warehouseId}",
        //     delete(delete_warehouse).get(get_warehouse),
        // )
        // .route(
        //     "/warehouses/{warehouseId}/databases/{databaseName}",
        //     get(get_database).delete(delete_database),
        // )
        // .route("/warehouses/{warehouseId}/databases", post(create_database))
        // .route(
        //     "/warehouses/{warehouseId}/databases/{databaseName}/register",
        //     post(register_table),
        // )
        // .route(
        //     "/warehouses/{warehouseId}/databases/{databaseName}/tables",
        //     post(create_table),
        // )
        // .route(
        //     "/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}",
        //     get(get_table).delete(delete_table),
        // )
        .route("/query", post(query))
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
