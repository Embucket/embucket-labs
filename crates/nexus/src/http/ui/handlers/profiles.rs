use crate::error::AppError;
use crate::http::ui::models::{aws, storage_profile};
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
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
        )
    ),
    tags(
        (name = "Storage profiles", description = "Storage profiles management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    operation_id = "webCreateStorageProfile",
    path = "/ui/storage-profiles",
    request_body = storage_profile::CreateStorageProfilePayload,
    responses(
        (status = 200, description = "Successful Response", body = storage_profile::StorageProfile),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn create_storage_profile(
    State(state): State<AppState>,
    Json(payload): Json<storage_profile::CreateStorageProfilePayload>,
) -> Result<Json<storage_profile::StorageProfile>, AppError> {
    Ok(Json(storage_profile::StorageProfile {
        r#type: aws::CloudProvider::S3,
        region: "2".to_string(),
        bucket: "".to_string(),
        credentials: Default::default(),
        sts_role_arn: None,
        endpoint: None,
        id: Default::default(),
        created_at: Default::default(),
        updated_at: Default::default(),
    }))
}

#[utoipa::path(
    get,
    operation_id = "webGetStorageProfile",
    path = "/ui/storage-profiles/{storageProfileId}",
    params(
        ("storageProfileId" = Uuid, Path, description = "Storage profile ID")
    ),
    responses(
        (status = 200, description = "Successful Response", body = storage_profile::StorageProfile),
        (status = 404, description = "Not found"),
    )
)]
pub async fn get_storage_profile(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<storage_profile::StorageProfile>, AppError> {
    Ok(Json(storage_profile::StorageProfile {
        r#type: aws::CloudProvider::S3,
        region: "1".to_string(),
        bucket: "".to_string(),
        credentials: Default::default(),
        sts_role_arn: None,
        endpoint: None,
        id: Default::default(),
        created_at: Default::default(),
        updated_at: Default::default(),
    }))
}

#[utoipa::path(
    delete,
    operation_id = "webDeleteStorageProfile",
    path = "/ui/storage-profiles/{storageProfileId}",
    params(
        ("storageProfileId" = Uuid, Path, description = "Storage profile ID")
    ),
    responses(
        (status = 200, description = "Successful Response", body = storage_profile::StorageProfile),
        (status = 404, description = "Not found"),
    )
)]
pub async fn delete_storage_profile(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<()>, AppError> {
    Ok(Json(()))
}

#[utoipa::path(
    get,
    operation_id = "webListStorageProfiles",
    path = "/ui/storage-profiles/",
    responses(
        (status = 200, body = Vec<storage_profile::StorageProfile>),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn list_storage_profiles(
    State(state): State<AppState>,
) -> Result<Json<Vec<storage_profile::StorageProfile>>, AppError> {
    Ok(Json(vec![]))
}
