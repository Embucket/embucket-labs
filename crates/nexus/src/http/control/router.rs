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

use crate::state::AppState;
use axum::routing::{delete, get, post};
use axum::Router;

use crate::http::control::handlers::storage_profiles::{
    create_storage_profile, delete_storage_profile, get_storage_profile, list_storage_profiles,
};
use crate::http::control::handlers::warehouses::{
    create_warehouse, delete_warehouse, get_warehouse, list_warehouses,
};

pub fn create_router() -> Router<AppState> {
    let sp_router = Router::new()
        .route("/", post(create_storage_profile))
        .route("/{id}", get(get_storage_profile))
        .route("/{id}", delete(delete_storage_profile))
        .route("/", get(list_storage_profiles));

    let wh_router = Router::new()
        .route("/", post(create_warehouse))
        .route("/{id}", get(get_warehouse))
        .route("/{id}", delete(delete_warehouse))
        .route("/", get(list_warehouses));

    Router::new()
        .nest("/v1/storage-profile", sp_router)
        .nest("/v1/warehouse", wh_router)
}
