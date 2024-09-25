use axum::{Json, response::IntoResponse, extract::State};
use axum_macros::debug_handler;
use crate::schemas::storage_profiles::{CreateStorageProfilePayload, StorageProfile};
use uuid::Uuid;
use chrono::Utc;

use crate::state::AppState;


#[debug_handler]
pub async fn create_storage_profile(
    State(_): State<AppState>,
    Json(payload): Json<CreateStorageProfilePayload>,
) -> impl IntoResponse {
    
    let response = StorageProfile {
        id: Uuid::new_v4(),
        provider_type: payload.provider_type,
        region: payload.region,
        bucket: payload.bucket,
        credentials: payload.credentials,
        sts_role_arn: payload.sts_role_arn,
        endpoint: payload.endpoint,
        created_at: Utc::now().naive_utc(),
        updated_at: Utc::now().naive_utc(),
        
    };

    Json(response)
}