use crate::error::AppError;
use crate::http::ui::models::warehouse;
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use control_plane::models::{Warehouse as WarehouseModel, WarehouseCreateRequest};
use utoipa::OpenApi;
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_warehouse,
        list_warehouses,
        create_warehouse,
        delete_warehouse,
    ),
    components(
        schemas(
            warehouse::Warehouse,
            warehouse::WarehousesDashboard,
            warehouse::CreateWarehousePayload,
        )
    ),
    tags(
        (name = "Warehouse", description = "Warehouse management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/warehouses",
    operation_id = "webWarehousesDashboard",
    responses(
        (status = 200, description = "List all warehouses", body = warehouse::WarehousesDashboard),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn list_warehouses(
    State(state): State<AppState>,
) -> Result<Json<warehouse::WarehousesDashboard>, AppError> {
    let warehouses = state.control_svc.list_warehouses().await?;
    let storage_profiles = state.control_svc.list_profiles().await?;
    let mut extended_warehouses = Vec::new();

    for warehouse in warehouses {
        let mut extended_warehouse =
            warehouse::WarehouseExtended::new(warehouse.clone().into(), Default::default());
        if let Some(profile) = storage_profiles
            .iter()
            .find(|p| p.id == extended_warehouse.storage_profile_id)
        {
            extended_warehouse.storage_profile = profile.clone().into();
            extended_warehouses.push(extended_warehouse)
        }
    }
    Ok(Json(warehouse::WarehousesDashboard {
        warehouses: extended_warehouses,
        statistics: Default::default(),
        compaction_summary: None,
    }))
}

#[utoipa::path(
    get,
    path = "/ui/warehouses/{warehouseId}",
    operation_id = "webWarehouseDashboard",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID")
    ),
    responses(
        (status = 200, description = "Warehouse found", body = warehouse::WarehouseExtended),
        (status = 404, description = "Warehouse not found")
    )
)]
pub async fn get_warehouse(
    State(state): State<AppState>,
    Path(warehouse_id): Path<Uuid>,
) -> Result<Json<warehouse::WarehouseExtended>, AppError> {
    let warehouse = state.control_svc.get_warehouse(warehouse_id).await?;

    let mut extended_warehouse =
        warehouse::WarehouseExtended::new(warehouse.into(), Default::default());

    if let Ok(profile) = state
        .control_svc
        .get_profile(extended_warehouse.storage_profile_id)
        .await
    {
        extended_warehouse.storage_profile = profile.into();
    }

    Ok(Json(extended_warehouse))
}

#[utoipa::path(
    post,
    path = "/ui/warehouses",
    request_body = warehouse::CreateWarehousePayload,
    operation_id = "webCreateWarehouse",
    responses(
        (status = 201, description = "Warehouse created", body = warehouse::Warehouse),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn create_warehouse(
    State(state): State<AppState>,
    Json(payload): Json<warehouse::CreateWarehousePayload>,
) -> Result<Json<warehouse::Warehouse>, AppError> {
    let request: WarehouseCreateRequest = payload.into();

    state
        .control_svc
        .get_profile(request.storage_profile_id)
        .await
        .map_err(|e| AppError::from(e))?;
    let warehouse: WarehouseModel = state
        .control_svc
        .create_warehouse(&request)
        .await
        .map_err(|e| AppError::from(e))?;
    Ok(Json(warehouse.into()))
}

#[utoipa::path(
    delete,
    path = "/ui/warehouses/{warehouseId}",
    operation_id = "webCreteWarehouse",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID")
    ),
    responses(
        (status = 204, description = "Warehouse deleted"),
        (status = 404, description = "Warehouse not found")
    )
)]
pub async fn delete_warehouse(
    State(state): State<AppState>,
    Path(warehouse_id): Path<Uuid>,
) -> Result<Json<()>, AppError> {
    state
        .control_svc
        .delete_warehouse(warehouse_id)
        .await
        .map_err(|e| AppError::from(e))?;
    Ok(Json(()))
}
