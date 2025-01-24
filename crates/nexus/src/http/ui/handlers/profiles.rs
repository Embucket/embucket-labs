use super::super::models::error::{self as model_error, NexusError, NexusResult};
use crate::http::ui::models::{aws, storage_profile};
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use control_plane::models::{StorageProfile, StorageProfileCreateRequest};
use snafu::ResultExt;
use utoipa::OpenApi;
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_storage_profile,
        get_storage_profile,
        delete_storage_profile,
        list_storage_profiles,
    ),
    components(
        schemas(
            storage_profile::CreateStorageProfilePayload,
            storage_profile::StorageProfile,
            aws::Credentials,
            aws::AwsAccessKeyCredential,
            aws::AwsRoleCredential,
            aws::CloudProvider,
            NexusError,
        )
    ),
    tags(
        (name = "storage_profiles", description = "Storage profiles management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    operation_id = "createStorageProfile",
    tags = ["storage_profiles"],
    path = "/ui/storage-profiles",
    request_body = storage_profile::CreateStorageProfilePayload,
    responses(
        (status = 200, description = "Successful Response", body = storage_profile::StorageProfile),
        (status = 400, description = "Bad request", body = NexusError),
        (status = 422, description = "Unprocessable entity", body = NexusError),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_storage_profile(
    State(state): State<AppState>,
    Json(payload): Json<storage_profile::CreateStorageProfilePayload>,
) -> NexusResult<Json<storage_profile::StorageProfile>> {
    let request: StorageProfileCreateRequest = payload.into();
    let profile: StorageProfile = state
        .control_svc
        .create_profile(&request)
        .await
        .context(model_error::StorageProfileCreateSnafu)?;
    Ok(Json(profile.into()))
}

#[utoipa::path(
    get,
    operation_id = "getStorageProfile",
    tags = ["storage_profiles"],
    path = "/ui/storage-profiles/{storageProfileId}",
    params(
        ("storageProfileId" = Uuid, Path, description = "Storage profile ID")
    ),
    responses(
        (status = 200, description = "Successful Response", body = storage_profile::StorageProfile),
        (status = 404, description = "Not found", body = NexusError),
        (status = 422, description = "Unprocessable entity", body = NexusError),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_storage_profile(
    State(state): State<AppState>,
    Path(storage_profile_id): Path<Uuid>,
) -> NexusResult<Json<storage_profile::StorageProfile>> {
    let profile = state.get_profile_by_id(storage_profile_id).await?;
    Ok(Json(profile))
}

#[utoipa::path(
    delete,
    operation_id = "deleteStorageProfile",
    tags = ["storage_profiles"],
    path = "/ui/storage-profiles/{storageProfileId}",
    params(
        ("storageProfileId" = Uuid, Path, description = "Storage profile ID")
    ),
    responses(
        (status = 200, description = "Successful Response", body = storage_profile::StorageProfile),
        (status = 404, description = "Not found", body = NexusError),
        (status = 422, description = "Unprocessable entity", body = NexusError),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_storage_profile(
    State(state): State<AppState>,
    Path(storage_profile_id): Path<Uuid>,
) -> NexusResult<Json<()>> {
    state
        .control_svc
        .delete_profile(storage_profile_id)
        .await
        .context(model_error::StorageProfileDeleteSnafu {
            id: storage_profile_id,
        })?;
    Ok(Json(()))
}

#[utoipa::path(
    get,
    operation_id = "listStorageProfiles",
    tags = ["storage_profiles"],
    path = "/ui/storage-profiles",
    responses(
        (status = 200, body = Vec<storage_profile::StorageProfile>),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_storage_profiles(
    State(state): State<AppState>,
) -> NexusResult<Json<Vec<storage_profile::StorageProfile>>> {
    let profiles = state
        .control_svc
        .list_profiles()
        .await
        .context(model_error::StorageProfileListSnafu)?;
    Ok(Json(profiles.into_iter().map(Into::into).collect()))
}
