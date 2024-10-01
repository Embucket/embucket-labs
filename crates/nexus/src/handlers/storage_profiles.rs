use crate::schemas::storage_profiles::{
    CreateStorageProfilePayload, StorageProfile as StorageProfileSchema,
};
use axum::{extract::Path, extract::State, Json};
use control_plane::models::{StorageProfile, StorageProfileCreateRequest};
use std::result::Result;
use uuid::Uuid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::error::AppError;
use crate::state::AppState;


#[derive(OpenApi)]
#[openapi(
    paths(create_storage_profile, get_storage_profile, delete_storage_profile, list_storage_profiles,),
    components(schemas(CreateStorageProfilePayload, StorageProfileSchema))
)]
pub struct StorageProfileApi;


#[utoipa::path(
    post, 
    path = "", 
    responses((status = 200, body = StorageProfileSchema))
)]
pub async fn create_storage_profile(
    State(state): State<AppState>,
    Json(payload): Json<CreateStorageProfilePayload>,
) -> Result<Json<StorageProfileSchema>, AppError> {
    let request: StorageProfileCreateRequest = payload.into();
    let profile: StorageProfile = state.control_svc.create_profile(&request).await?;

    Ok(Json(profile.into()))
}

#[utoipa::path(
    get, 
    path = "", 
    responses((status = 200, body = StorageProfileSchema))
)]
pub async fn get_storage_profile(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<StorageProfileSchema>, AppError> {
    let profile = state.control_svc.get_profile(id).await?;

    Ok(Json(profile.into()))
}

#[utoipa::path(
    delete, 
    path = "", 
    responses((status = 200))
)]
pub async fn delete_storage_profile(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<()>, AppError> {
    state.control_svc.delete_profile(id).await?;

    Ok(Json(()))
}

#[utoipa::path(
    get, 
    path = "", 
    responses((status = 200, body = Vec<StorageProfileSchema>))
)]
pub async fn list_storage_profiles(
    State(state): State<AppState>,
) -> Result<Json<Vec<StorageProfileSchema>>, AppError> {
    let profiles = state.control_svc.list_profiles().await?;

    Ok(Json(profiles.into_iter().map(|p| p.into()).collect()))
}
