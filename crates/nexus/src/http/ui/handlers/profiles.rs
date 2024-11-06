use crate::http::ui::models::errors::AppError;
use crate::http::ui::models::{aws, storage_profile};
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use control_plane::models::{StorageProfile, StorageProfileCreateRequest};
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
            AppError,
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
        (status = 400, description = "Bad request", body = AppError),
        (status = 422, description = "Unprocessable entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn create_storage_profile(
    State(state): State<AppState>,
    Json(payload): Json<storage_profile::CreateStorageProfilePayload>,
) -> Result<Json<storage_profile::StorageProfile>, AppError> {
    let request: StorageProfileCreateRequest = payload.into();
    let profile: StorageProfile =
        state
            .control_svc
            .create_profile(&request)
            .await
            .map_err(|e| {
                let fmt = format!("{}: failed to create storage profile", e);
                AppError::new(e, fmt.as_str())
            })?;
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
        (status = 404, description = "Not found", body = AppError),
        (status = 422, description = "Unprocessable entity", body = AppError),
    )
)]
pub async fn get_storage_profile(
    State(state): State<AppState>,
    Path(storage_profile_id): Path<Uuid>,
) -> Result<Json<storage_profile::StorageProfile>, AppError> {
    let profile = state.get_profile_by_id(storage_profile_id).await?;
    Ok(Json(profile.into()))
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
        (status = 404, description = "Not found", body = AppError),
        (status = 422, description = "Unprocessable entity", body = AppError),
    )
)]
pub async fn delete_storage_profile(
    State(state): State<AppState>,
    Path(storage_profile_id): Path<Uuid>,
) -> Result<Json<()>, AppError> {
    state
        .control_svc
        .delete_profile(storage_profile_id)
        .await
        .map_err(|e| {
            let fmt = format!(
                "{}: failed to delete storage profile with id {}",
                e, storage_profile_id
            );
            AppError::new(e, fmt.as_str())
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
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn list_storage_profiles(
    State(state): State<AppState>,
) -> Result<Json<Vec<storage_profile::StorageProfile>>, AppError> {
    let profiles = state.control_svc.list_profiles().await.map_err(|e| {
        let fmt = format!("{}: failed to list storage profile", e);
        AppError::new(e, fmt.as_str())
    })?;
    Ok(Json(profiles.into_iter().map(|p| p.into()).collect()))
}
