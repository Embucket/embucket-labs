use std::result::Result;
use axum::{Json, extract::State, extract::Path};
use axum_macros::debug_handler;
use uuid::Uuid;
use crate::schemas::storage_profiles::{CreateStorageProfilePayload, StorageProfile as StorageProfileSchema};
use control_plane::models::{StorageProfileCreateRequest, StorageProfile};

use crate::state::AppState;
use crate::error::AppError;


#[debug_handler]
pub async fn create_storage_profile(
    State(state): State<AppState>,
    Json(payload): Json<CreateStorageProfilePayload>,
) -> Result<Json<StorageProfileSchema>, AppError> {
    let request: StorageProfileCreateRequest = payload.into();
    let profile: StorageProfile = state.storage_profile_svc.create_profile(&request).await?;
    
    Ok(Json(profile.into()))
}

pub async fn get_storage_profile(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<StorageProfileSchema>, AppError> {
    let profile = state.storage_profile_svc.get_profile(id).await?;
    
    Ok(Json(profile.into()))
}

pub async fn delete_storage_profile(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<()>, AppError> {
    state.storage_profile_svc.delete_profile(id).await?;
    
    Ok(Json(()))
}

pub async fn list_storage_profiles(
    State(state): State<AppState>,
) -> Result<Json<Vec<StorageProfileSchema>>, AppError> {
    let profiles = state.storage_profile_svc.list_profiles().await?;
    
    Ok(Json(profiles.into_iter().map(|p| p.into()).collect()))
}