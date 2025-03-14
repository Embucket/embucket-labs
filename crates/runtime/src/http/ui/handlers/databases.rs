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
use crate::http::{
    error::ErrorResponse,
    metastore::handlers::QueryParameters,
    ui::error::{UIError, UIResult},
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use icebucket_metastore::error::{self as metastore_error, MetastoreError};
use icebucket_metastore::models::IceBucketDatabase;
use snafu::ResultExt;
use utoipa::OpenApi;
use validator::Validate;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_database,
        get_database,
        delete_database,
        list_databases,
        update_database,
    ),
    components(
        schemas(
            IceBucketDatabase,
            ErrorResponse,
        )
    ),
    tags(
        (name = "databases", description = "Databases management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    operation_id = "createDatabase",
    tags = ["databases"],
    path = "/ui/databases",
    request_body = IceBucketDatabase,
    responses(
        (status = 200, description = "Successful Response", body = IceBucketDatabase),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_database(
    State(state): State<AppState>,
    Json(database): Json<IceBucketDatabase>,
) -> UIResult<Json<IceBucketDatabase>> {
    database
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .create_database(&database.ident.clone(), database)
        .await
        .map_err(|e| UIError::Metastore { source: e })
        .map(|o| Json(o.data))
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
        (status = 200, description = "Successful Response", body = IceBucketDatabase),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> UIResult<Json<IceBucketDatabase>> {
    match state.metastore.get_database(&database_name).await {
        Ok(Some(db)) => Ok(Json(db.data)),
        Ok(None) => Err(MetastoreError::DatabaseNotFound {
            db: database_name.clone(),
        }
        .into()),
        Err(e) => Err(e.into()),
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
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_database(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(database_name): Path<String>,
) -> UIResult<()> {
    state
        .metastore
        .delete_database(&database_name, query.cascade.unwrap_or_default())
        .await
        .map_err(|e| UIError::Metastore { source: e })
}

#[utoipa::path(
    put,
    operation_id = "updateDatabase",
    tags = ["databases"],
    path = "/ui/databases/{databaseName}",
    params(
        ("databaseName" = String, Path, description = "Database name")
    ),
    responses(
        (status = 200, description = "Successful Response", body = IceBucketDatabase),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(database): Json<IceBucketDatabase>,
) -> UIResult<Json<IceBucketDatabase>> {
    database
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    //TODO: Implement database renames
    state
        .metastore
        .update_database(&database_name, database)
        .await
        .map_err(|e| UIError::Metastore { source: e })
        .map(|o| Json(o.data))
}

#[utoipa::path(
    get,
    operation_id = "listDatabases",
    tags = ["databases"],
    path = "/ui/databases",
    responses(
        (status = 200, body = Vec<IceBucketDatabase>),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_databases(
    State(state): State<AppState>,
) -> UIResult<Json<Vec<IceBucketDatabase>>> {
    let res = state
        .metastore
        .list_databases()
        .await
        .map_err(|e| UIError::Metastore { source: e })
        // TODO: use deref
        .map(|o| Json(o.iter().map(|x| x.data.clone()).collect()))?;
    Ok(res)
}
