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

use crate::http::state::AppState;
use crate::http::ui::models::databases_navigation::{
    NavigationDatabase, NavigationSchema, NavigationTable,
};
use crate::http::{
    error::ErrorResponse,
    ui::error::{UIError, UIResult},
};
use axum::{extract::State, Json};
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_databases_navigation,
    ),
    components(
        schemas(
            NavigationDatabase,
            ErrorResponse,
        )
    ),
    tags(
        (name = "databases-navigation", description = "Databases navigation endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    operation_id = "getDatabasesNavigation",
    tags = ["databases-navigation"],
    path = "/ui/databases-navigation",
    responses(
        (status = 200, description = "Successful Response", body = Vec<NavigationDatabase>),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_databases_navigation(
    State(state): State<AppState>,
) -> UIResult<Json<Vec<NavigationDatabase>>> {
    let rw_databases = state
        .metastore
        .list_databases()
        .await
        .map_err(|e| UIError::Metastore { source: e })?;

    let mut databases: Vec<NavigationDatabase> = vec![];
    for rw_database in rw_databases {
        let rw_schemas = state
            .metastore
            .list_schemas(&rw_database.ident)
            .await
            .map_err(|e| UIError::Metastore { source: e })?;

        let mut schemas: Vec<NavigationSchema> = vec![];
        for rw_schema in rw_schemas {
            let rw_tables = state
                .metastore
                .list_tables(&rw_schema.ident)
                .await
                .map_err(|e| UIError::Metastore { source: e })?;

            let mut tables: Vec<NavigationTable> = vec![];
            for rw_table in rw_tables {
                tables.push(NavigationTable {
                    name: rw_table.ident.table.clone(),
                });
            }
            schemas.push(NavigationSchema {
                name: rw_schema.ident.schema.clone(),
                tables,
            });
        }
        databases.push(NavigationDatabase {
            name: rw_database.ident.clone(),
            schemas,
        });
    }

    Ok(Json(databases))
}
