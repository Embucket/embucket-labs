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

use crate::http::control::schemas::storage_profiles::{
    AwsAccessKeyCredential, AwsRoleCredential, CloudProvider, CreateStorageProfilePayload,
    Credentials, StorageProfile,
};
use axum::{extract::Path, extract::State, Json};
use control_plane::models::{StorageProfile as StorageProfileModel, StorageProfileCreateRequest};
use std::result::Result;
use utoipa::OpenApi;
use uuid::Uuid;

use crate::error::AppError;
use crate::state::AppState;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_storage_profile,
        get_storage_profile,
        delete_storage_profile,
        list_storage_profiles,
    ),
    components(schemas(
        CreateStorageProfilePayload,
        StorageProfile,
        Credentials,
        AwsAccessKeyCredential,
        AwsRoleCredential,
        CloudProvider
    ),)
)]
pub struct StorageProfileApi;

#[utoipa::path(
    post,
    operation_id = "createStorageProfile",
    path = "", 
    request_body = CreateStorageProfilePayload,
    responses((status = 200, body = StorageProfile))
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_storage_profile(
    State(state): State<AppState>,
    Json(payload): Json<CreateStorageProfilePayload>,
) -> Result<Json<StorageProfile>, AppError> {
    let request: StorageProfileCreateRequest = payload.into();
    let profile: StorageProfileModel = state.control_svc.create_profile(&request).await?;

    Ok(Json(profile.into()))
}

#[utoipa::path(
    get,
    operation_id = "getStorageProfile",
    path = "/{storageProfileId}", 
    params(("storageProfileId" = Uuid, description = "Storage profile ID")),
    responses((status = 200, body = StorageProfile)),
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_storage_profile(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<StorageProfile>, AppError> {
    let profile = state.control_svc.get_profile(id).await?;

    Ok(Json(profile.into()))
}

#[utoipa::path(
    delete,
    operation_id = "deleteStorageProfile",
    path = "/{storageProfileId}", 
    params(("storageProfileId" = Uuid, description = "Storage profile ID")),
    responses((status = 200))
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_storage_profile(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<()>, AppError> {
    state.control_svc.delete_profile(id).await?;

    Ok(Json(()))
}

#[utoipa::path(
    get,
    operation_id = "listStorageProfiles",
    path = "", 
    responses((status = 200, body = Vec<StorageProfile>))
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_storage_profiles(
    State(state): State<AppState>,
) -> Result<Json<Vec<StorageProfile>>, AppError> {
    let profiles = state.control_svc.list_profiles().await?;

    Ok(Json(profiles.into_iter().map(Into::into).collect()))
}
