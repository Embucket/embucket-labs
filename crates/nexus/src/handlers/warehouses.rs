use std::result::Result;
use axum::{Json, extract::State, extract::Path};
use axum_macros::debug_handler;
use uuid::Uuid;
use crate::schemas::warehouses::{CreateWarehouseRequest as CreateWarehouseSchema, Warehouse as WarehouseSchema};
use control_plane::models::{WarehouseCreateRequest, Warehouse};

use crate::state::AppState;
use crate::error::AppError;


#[debug_handler]
pub async fn create_warehouse(
    State(state): State<AppState>,
    Json(payload): Json<CreateWarehouseSchema>,
) -> Result<Json<WarehouseSchema>, AppError> {
    let request: WarehouseCreateRequest = payload.into();
    let profile: Warehouse = state.control_svc.create_warehouse(&request).await?;
    
    Ok(Json(profile.into()))
}

pub async fn get_warehouse(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<WarehouseSchema>, AppError> {
    let profile = state.control_svc.get_warehouse(id).await?;
    
    Ok(Json(profile.into()))
}

pub async fn delete_warehouse(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<()>, AppError> {
    state.control_svc.delete_warehouse(id).await?;
    
    Ok(Json(()))
}

pub async fn list_warehouses(
    State(state): State<AppState>,
) -> Result<Json<Vec<WarehouseSchema>>, AppError> {
    let profiles = state.control_svc.list_warehouses().await?;
    
    Ok(Json(profiles.into_iter().map(|p| p.into()).collect()))
}