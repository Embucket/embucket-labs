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
use crate::http::ui::databases::models::DatabasesParameters;
use crate::http::{
    error::ErrorResponse,
    metastore::handlers::QueryParameters,
    ui::databases::error::{DatabasesAPIError, DatabasesResult},
    ui::databases::models::{
        Database, DatabaseCreatePayload, DatabaseCreateResponse, DatabaseResponse,
        DatabaseUpdatePayload, DatabaseUpdateResponse, DatabasesResponse,
    },
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use icebucket_metastore::error::MetastoreError;
use icebucket_metastore::IceBucketDatabase;
use utoipa::OpenApi;
use validator::Validate;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_database,
        get_database,
        delete_database,
        list_databases,
        // update_database,
    ),
    components(
        schemas(
            DatabaseCreatePayload,
            DatabaseCreateResponse,
            DatabaseResponse,
            DatabasesResponse,
            Database,
            ErrorResponse,
        )
    ),
    tags(
        (name = "databases", description = "Databases endpoints")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    operation_id = "createDatabase",
    tags = ["databases"],
    path = "/ui/databases",
    request_body = DatabaseCreatePayload,
    responses(
        (status = 200, description = "Successful Response", body = DatabaseCreateResponse),
        (status = 409, description = "Already Exists", body = ErrorResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_database(
    State(state): State<AppState>,
    Json(database): Json<DatabaseCreatePayload>,
) -> DatabasesResult<Json<DatabaseCreateResponse>> {
    let database: IceBucketDatabase = database.data.into();
    database.validate().map_err(|e| DatabasesAPIError::Create {
        source: MetastoreError::Validation { source: e },
    })?;
    state
        .metastore
        .create_database(&database.ident.clone(), database)
        .await
        .map_err(|e| DatabasesAPIError::Create { source: e })
        .map(|o| {
            Json(DatabaseCreateResponse {
                data: o.data.into(),
            })
        })
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
        (status = 200, description = "Successful Response", body = DatabaseResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> DatabasesResult<Json<DatabaseResponse>> {
    match state.metastore.get_database(&database_name).await {
        Ok(Some(db)) => Ok(Json(DatabaseResponse {
            data: db.data.into(),
        })),
        Ok(None) => Err(DatabasesAPIError::Get {
            source: MetastoreError::DatabaseNotFound {
                db: database_name.clone(),
            },
        }),
        Err(e) => Err(DatabasesAPIError::Get { source: e }),
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
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_database(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(database_name): Path<String>,
) -> DatabasesResult<()> {
    state
        .metastore
        .delete_database(&database_name, query.cascade.unwrap_or_default())
        .await
        .map_err(|e| DatabasesAPIError::Delete { source: e })
}

#[utoipa::path(
    put,
    operation_id = "updateDatabase",
    tags = ["databases"],
    path = "/ui/databases/{databaseName}",
    request_body = DatabaseUpdatePayload,
    params(
        ("databaseName" = String, Path, description = "Database name")
    ),
    responses(
        (status = 200, description = "Successful Response", body = DatabaseUpdateResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 400, description = "Invalid data", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(database): Json<DatabaseUpdatePayload>,
) -> DatabasesResult<Json<DatabaseUpdateResponse>> {
    let database: IceBucketDatabase = database.data.into();
    database.validate().map_err(|e| DatabasesAPIError::Update {
        source: MetastoreError::Validation { source: e },
    })?;
    //TODO: Implement database renames
    state
        .metastore
        .update_database(&database_name, database)
        .await
        .map_err(|e| DatabasesAPIError::Update { source: e })
        .map(|o| {
            Json(DatabaseUpdateResponse {
                data: o.data.into(),
            })
        })
}

#[utoipa::path(
    get,
    operation_id = "getDatabases",
    params(
        ("cursor" = Option<String>, Query, description = "Databases cursor"),
        ("limit" = Option<usize>, Query, description = "Databases limit"),
    ),
    tags = ["databases"],
    path = "/ui/databases",
    responses(
        (status = 200, body = DatabasesResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_databases(
    Query(parameters): Query<DatabasesParameters>,
    State(state): State<AppState>,
) -> DatabasesResult<Json<DatabasesResponse>> {
    state
        .metastore
        .list_databases(parameters.cursor.clone(), parameters.limit)
        .await
        .map_err(|e| DatabasesAPIError::List { source: e })
        .map(|o| {
            let next_cursor = o
                .iter()
                .last()
                .map_or(String::new(), |rw_object| rw_object.ident.clone());
            Json(DatabasesResponse {
                items: o.into_iter().map(|x| x.data.into()).collect(),
                current_cursor: parameters.cursor,
                next_cursor,
            })
        })
}
