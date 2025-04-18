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
use crate::http::ui::dashboard::error::{DashboardAPIError, DashboardResult};
use crate::http::ui::dashboard::models::{Dashboard, DashboardResponse};
use crate::http::ui::queries::error::QueryError;
use axum::{extract::State, Json};
use embucket_metastore::error::MetastoreError;
use embucket_utils::scan_iterator::ScanIterator;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_dashboard,
    ),
    components(
        schemas(
            DashboardResponse,
            Dashboard,
        )
    ),
    tags(
        (name = "dashboard", description = "Dashboard endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    operation_id = "getDashboard",
    tags = ["dashboard"],
    path = "/ui/dashboard",
    responses(
        (status = 200, description = "Successful Response", body = DashboardResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_dashboard(
    State(state): State<AppState>,
) -> DashboardResult<Json<DashboardResponse>> {
    let rw_databases = state
        .metastore
        .iter_databases()
        .collect()
        .await
        .map_err(|e| DashboardAPIError::Metastore {
            source: MetastoreError::UtilSlateDB { source: e },
        })?;
    let total_databases = rw_databases.len();
    let mut total_schemas = 0;
    let mut total_tables = 0;
    for rw_database in rw_databases {
        let rw_schemas = state
            .metastore
            .iter_schemas(&rw_database.ident.clone())
            .collect()
            .await
            .map_err(|e| DashboardAPIError::Metastore {
                source: MetastoreError::UtilSlateDB { source: e },
            })?;
        total_schemas += rw_schemas.len();
        for rw_schema in rw_schemas {
            total_tables += state
                .metastore
                .iter_tables(&rw_schema.ident)
                .collect()
                .await
                .map_err(|e| DashboardAPIError::Metastore {
                    source: MetastoreError::UtilSlateDB { source: e },
                })?
                .len();
        }
    }

    let total_queries = state
        .history
        .get_queries(None, None, None)
        .await
        .map_err(|e| DashboardAPIError::Queries {
            source: QueryError::Store { source: e },
        })?
        .len();

    Ok(Json(DashboardResponse {
        data: Dashboard {
            total_databases,
            total_schemas,
            total_tables,
            total_queries,
        },
    }))
}
