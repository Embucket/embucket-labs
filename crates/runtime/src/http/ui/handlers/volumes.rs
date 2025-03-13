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
use icebucket_metastore::models::IceBucketVolume;
use snafu::ResultExt;
use utoipa::OpenApi;
use validator::Validate;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_volume,
        get_volume,
        delete_volume,
        list_volumes,
        update_volume,
    ),
    components(
        schemas(
            IceBucketVolume,
            ErrorResponse,
        )
    ),
    tags(
        (name = "volumes", description = "Volumes management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    operation_id = "createVolume",
    tags = ["volumes"],
    path = "/ui/volumes",
    request_body = IceBucketVolume,
    responses(
        (status = 200, description = "Successful Response", body = IceBucketVolume),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_volume(
    State(state): State<AppState>,
    Json(volume): Json<IceBucketVolume>,
) -> UIResult<Json<IceBucketVolume>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .create_volume(&volume.ident.clone(), volume)
        .await
        .map_err(|e| UIError::Metastore { source: e })
        .map(|o| Json(o.data))
}

#[utoipa::path(
    get,
    operation_id = "getVolume",
    tags = ["volumes"],
    path = "/ui/volumes/{volumeName}",
    params(
        ("volumeName" = String, Path, description = "Volume name")
    ),
    responses(
        (status = 200, description = "Successful Response", body = IceBucketVolume),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
) -> UIResult<Json<IceBucketVolume>> {
    match state.metastore.get_volume(&volume_name).await {
        Ok(Some(volume)) => Ok(Json(volume.data)),
        Ok(None) => Err(UIError::Metastore {
            source: MetastoreError::VolumeNotFound {
                volume: volume_name.clone(),
            },
        }),
        Err(e) => Err(e.into()),
    }
}

#[utoipa::path(
    delete,
    operation_id = "deleteVolume",
    tags = ["volumes"],
    path = "/ui/volumes/{volumeName}",
    params(
        ("volumeName" = String, Path, description = "Volume name")
    ),
    responses(
        (status = 200, description = "Successful Response"),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_volume(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(volume_name): Path<String>,
) -> UIResult<()> {
    state
        .metastore
        .delete_volume(&volume_name, query.cascade.unwrap_or_default())
        .await
        .map_err(|e| UIError::Metastore { source: e })
}

#[utoipa::path(
    get,
    operation_id = "updateVolume",
    tags = ["volumes"],
    path = "/ui/volumes/{volumeName}",
    params(
        ("volumeName" = String, Path, description = "Volume name")
    ),
    responses(
        (status = 200, description = "Successful Response", body = IceBucketVolume),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
    Json(volume): Json<IceBucketVolume>,
) -> UIResult<Json<IceBucketVolume>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .update_volume(&volume_name, volume)
        .await
        .map_err(|e| UIError::Metastore { source: e })
        .map(|o| Json(o.data))
}

#[utoipa::path(
    get,
    operation_id = "listVolumes",
    tags = ["volumes"],
    path = "/ui/volumes",
    responses(
        (status = 200, body = Vec<IceBucketVolume>),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_volumes(State(state): State<AppState>) -> UIResult<Json<Vec<IceBucketVolume>>> {
    let res = state
        .metastore
        .list_volumes()
        .await
        .map_err(|e| UIError::Metastore { source: e })
        // TODO: use deref
        .map(|o| Json(o.iter().map(|x| x.data.clone()).collect()))?;
    Ok(res)
}
