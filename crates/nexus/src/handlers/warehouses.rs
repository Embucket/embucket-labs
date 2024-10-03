use crate::schemas::warehouses::{
    CreateWarehouseRequest as CreateWarehouseSchema, Warehouse as WarehouseSchema,
};
use axum::{extract::Path, extract::State, Json};
use axum_macros::debug_handler;
use control_plane::models::{Warehouse, WarehouseCreateRequest};
use std::result::Result;
use utoipa::OpenApi;
use uuid::Uuid;

use crate::error::AppError;
use crate::state::AppState;


// #[derive(OpenApi)]
// #[openapi(
//     paths(create_storage_profile, get_storage_profile, delete_storage_profile, list_storage_profiles,),
//     components(schemas(CreateStorageProfilePayload, StorageProfileSchema, Credentials, AwsAccessKeyCredential, AwsRoleCredential, CloudProvider),)
// )]
// pub struct StorageProfileApi;

#[derive(OpenApi)]
#[openapi(
    paths(create_warehouse, get_warehouse, delete_warehouse, list_warehouses,),
    components(schemas(CreateWarehouseSchema, WarehouseSchema, ),)
)]
pub struct WarehouseApi;

#[utoipa::path(
    post, 
    operation_id = "createWarehouse",
    path = "", 
    request_body = CreateWarehouseSchema,
    responses((status = 200, body = WarehouseSchema))
)]
pub async fn create_warehouse(
    State(state): State<AppState>,
    Json(payload): Json<CreateWarehouseSchema>,
) -> Result<Json<WarehouseSchema>, AppError> {
    let request: WarehouseCreateRequest = payload.into();
    let profile: Warehouse = state.control_svc.create_warehouse(&request).await?;

    Ok(Json(profile.into()))
}

#[utoipa::path(
    post, 
    operation_id = "getWarehouse",
    path = "/{warehouseId}", 
    responses((status = 200, body = WarehouseSchema))
)]
pub async fn get_warehouse(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<WarehouseSchema>, AppError> {
    let profile = state.control_svc.get_warehouse(id).await?;

    Ok(Json(profile.into()))
}

#[utoipa::path(
    post, 
    operation_id = "deleteWarehouse",
    path = "/{warehouseId}", 
    responses((status = 200))
)]
pub async fn delete_warehouse(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<()>, AppError> {
    state.control_svc.delete_warehouse(id).await?;

    Ok(Json(()))
}

#[utoipa::path(
    post, 
    operation_id = "listWarehouses",
    path = "", 
    responses((status = 200, body = Vec<WarehouseSchema>))
)]
pub async fn list_warehouses(
    State(state): State<AppState>,
) -> Result<Json<Vec<WarehouseSchema>>, AppError> {
    let profiles = state.control_svc.list_warehouses().await?;

    Ok(Json(profiles.into_iter().map(|p| p.into()).collect()))
}
