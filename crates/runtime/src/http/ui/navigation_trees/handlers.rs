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

use crate::http::error::ErrorResponse;
use crate::http::state::AppState;
use crate::http::ui::navigation_trees::error::{NavigationTreesAPIError, NavigationTreesResult};
use crate::http::ui::navigation_trees::models::{
    NavigationTreeDatabase, NavigationTreeSchema, NavigationTreeTable, NavigationTreesParameters,
    NavigationTreesResponse,
};
use axum::extract::Query;
use axum::{extract::State, Json};
use icebucket_utils::list_config::ListConfig;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_navigation_trees,
    ),
    components(
        schemas(
            NavigationTreesResponse,
            NavigationTreeDatabase,
            NavigationTreeSchema,
            NavigationTreeTable,
            ErrorResponse,
        )
    ),
    tags(
        (name = "navigation-trees", description = "Navigation trees endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    operation_id = "getNavigationTrees",
    params(
        ("cursor" = Option<String>, Query, description = "Navigation trees cursor"),
        ("limit" = Option<usize>, Query, description = "Navigation trees limit"),
    ),
    tags = ["navigation-trees"],
    path = "/ui/navigation-trees",
    responses(
        (status = 200, description = "Successful Response", body = NavigationTreesResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_navigation_trees(
    Query(parameters): Query<NavigationTreesParameters>,
    State(state): State<AppState>,
) -> NavigationTreesResult<Json<NavigationTreesResponse>> {
    let rw_databases = state
        .metastore
        .list_databases(ListConfig::new(
            parameters.cursor.clone(),
            parameters.limit,
            None,
        ))
        .await
        .map_err(|e| NavigationTreesAPIError::Get { source: e })?;

    let next_cursor = rw_databases
        .iter()
        .last()
        .map_or(String::new(), |rw_object| rw_object.ident.clone());

    let mut databases: Vec<NavigationTreeDatabase> = vec![];
    for rw_database in rw_databases {
        let rw_schemas = state
            .metastore
            .list_schemas(&rw_database.ident.clone(), ListConfig::default())
            .await
            .map_err(|e| NavigationTreesAPIError::Get { source: e })?;

        let mut schemas: Vec<NavigationTreeSchema> = vec![];
        for rw_schema in rw_schemas {
            let rw_tables = state
                .metastore
                .list_tables(&rw_schema.ident, ListConfig::default())
                .await
                .map_err(|e| NavigationTreesAPIError::Get { source: e })?;

            let mut tables: Vec<NavigationTreeTable> = vec![];
            for rw_table in rw_tables {
                tables.push(NavigationTreeTable {
                    name: rw_table.ident.table.clone(),
                });
            }
            schemas.push(NavigationTreeSchema {
                name: rw_schema.ident.schema.clone(),
                tables,
            });
        }
        databases.push(NavigationTreeDatabase {
            name: rw_database.ident.clone(),
            schemas,
        });
    }

    Ok(Json(NavigationTreesResponse {
        items: databases,
        current_cursor: parameters.cursor,
        next_cursor,
    }))
}
