use crate::http::control::schemas::warehouses::{CreateWarehouseRequest, Warehouse};
use axum::{extract::Path, extract::State, Json};
use control_plane::models::{Warehouse as WarehouseModel, WarehouseCreateRequest};
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
    components(schemas(CreateWarehouseRequest, Warehouse,),)
)]
pub struct WarehouseApi;

#[utoipa::path(
    post,
    operation_id = "createWarehouse",
    path = "", 
    request_body = CreateWarehouseRequest,
    responses((status = 200, body = Warehouse))
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_warehouse(
    State(state): State<AppState>,
    Json(payload): Json<CreateWarehouseRequest>,
) -> Result<Json<Warehouse>, AppError> {
    let request: WarehouseCreateRequest = payload.into();
    let profile: WarehouseModel = state.control_svc.create_warehouse(&request).await?;

    Ok(Json(profile.into()))
}

#[utoipa::path(
    get,
    operation_id = "getWarehouse",
    path = "/{warehouseId}", 
    params(("warehouseId" = Uuid, description = "Warehouse ID")),
    responses((status = 200, body = Warehouse))
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_warehouse(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Warehouse>, AppError> {
    let profile = state.control_svc.get_warehouse(id).await?;

    Ok(Json(profile.into()))
}

#[utoipa::path(
    delete,
    operation_id = "deleteWarehouse",
    path = "/{warehouseId}", 
    params(("warehouseId" = Uuid, description = "Warehouse ID")),
    responses((status = 200))
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_warehouse(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<()>, AppError> {
    state.control_svc.delete_warehouse(id).await?;

    Ok(Json(()))
}

#[utoipa::path(
    get,
    operation_id = "listWarehouses",
    path = "", 
    responses((status = 200, body = Vec<Warehouse>))
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_warehouses(
    State(state): State<AppState>,
) -> Result<Json<Vec<Warehouse>>, AppError> {
    let profiles = state.control_svc.list_warehouses().await?;

    Ok(Json(profiles.into_iter().map(Into::into).collect()))
}
