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
    ui::volumes::errors::{VolumesAPIError, VolumesResult},
    ui::volumes::models::{VolumePayload, VolumeResponse, VolumesResponse},
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use icebucket_metastore::error::MetastoreError;
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
            VolumePayload,
            VolumeResponse,
            VolumesResponse,
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
    request_body = VolumePayload,
    responses(
        (status = 200, description = "Successful Response", body = VolumeResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_volume(
    State(state): State<AppState>,
    Json(volume): Json<VolumePayload>,
) -> VolumesResult<Json<VolumeResponse>> {
    volume
        .data
        .validate()
        .map_err(|e| VolumesAPIError::Create {
            source: MetastoreError::Validation { source: e },
        })?;
    state
        .metastore
        .create_volume(&volume.data.ident.clone(), volume.data)
        .await
        .map_err(|e| VolumesAPIError::Create { source: e })
        .map(|o| Json(VolumeResponse { data: o.data }))
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
        (status = 200, description = "Successful Response", body = VolumeResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
) -> VolumesResult<Json<VolumeResponse>> {
    match state.metastore.get_volume(&volume_name).await {
        Ok(Some(volume)) => Ok(Json(VolumeResponse { data: volume.data })),
        Ok(None) => Err(VolumesAPIError::Get {
            source: MetastoreError::VolumeNotFound {
                volume: volume_name.clone(),
            },
        }),
        Err(e) => Err(VolumesAPIError::Get { source: e }),
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
) -> VolumesResult<()> {
    state
        .metastore
        .delete_volume(&volume_name, query.cascade.unwrap_or_default())
        .await
        .map_err(|e| VolumesAPIError::Delete { source: e })
}

#[utoipa::path(
    put,
    operation_id = "updateVolume",
    tags = ["volumes"],
    path = "/ui/volumes/{volumeName}",
    params(
        ("volumeName" = String, Path, description = "Volume name")
    ),
    request_body = VolumePayload,
    responses(
        (status = 200, description = "Successful Response", body = VolumeResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
    Json(volume): Json<VolumePayload>,
) -> VolumesResult<Json<VolumeResponse>> {
    volume
        .data
        .validate()
        .map_err(|e| VolumesAPIError::Update {
            source: MetastoreError::Validation { source: e },
        })?;
    state
        .metastore
        .update_volume(&volume_name, volume.data)
        .await
        .map_err(|e| VolumesAPIError::Update { source: e })
        .map(|o| Json(VolumeResponse { data: o.data }))
}

#[utoipa::path(
    get,
    operation_id = "getVolumes",
    tags = ["volumes"],
    path = "/ui/volumes",
    responses(
        (status = 200, body = VolumesResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_volumes(State(state): State<AppState>) -> VolumesResult<Json<VolumesResponse>> {
    state
        .metastore
        .list_volumes()
        .await
        .map_err(|e| VolumesAPIError::List { source: e })
        // TODO: use deref
        .map(|o| {
            Json(VolumesResponse {
                items: o.iter().map(|x| x.data.clone()).collect(),
            })
        })
}
