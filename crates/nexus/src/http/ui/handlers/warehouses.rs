use crate::error::AppError;
use crate::http::ui::models::warehouse;
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
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
struct ApiDoc;

#[utoipa::path(
    get,
    path = "/warehouses",
    operation_id = "warehousesDashboard",
    responses(
        (status = 200, description = "List all warehouses", body = Vec<warehouse::WarehousesDashboard>),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn list_warehouses(
    State(state): State<AppState>,
) -> Result<Json<Vec<warehouse::WarehousesDashboard>>, AppError> {
    Ok(Json(vec![]))
}

#[utoipa::path(
    get,
    path = "/warehouses/{warehouseId}",
    operation_id = "warehouseDashboard",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID")
    ),
    responses(
        (status = 200, description = "Warehouse found", body = warehouse::Warehouse),
        (status = 404, description = "Warehouse not found")
    )
)]
pub async fn get_warehouse(
    State(state): State<AppState>,
    Path(warehouse_id): Path<Uuid>,
) -> Result<Json<warehouse::Warehouse>, AppError> {
    // let warehouse = state.warehouse_service.get_warehouse(warehouse_id).await?;
    Ok(Json(warehouse::Warehouse {
        name: "".to_string(),
        storage_profile_id: Default::default(),
        key_prefix: "key".to_string(),
        id: Default::default(),
        external_id: Default::default(),
        location: "".to_string(),
        created_at: Default::default(),
        updated_at: Default::default(),
    }))
}

#[utoipa::path(
    post,
    path = "/warehouses",
    request_body = warehouse::CreateWarehousePayload,
    operation_id = "createWarehouse",
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
    Ok(Json(warehouse::Warehouse {
        name: "".to_string(),
        storage_profile_id: Default::default(),
        key_prefix: "".to_string(),
        id: Default::default(),
        external_id: Default::default(),
        location: "".to_string(),
        created_at: Default::default(),
        updated_at: Default::default(),
    }))
}

#[utoipa::path(
    delete,
    path = "/warehouses/{warehouseId}",
    operation_id = "deleteWarehouse",
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
) -> Result<(), AppError> {
    Ok(())
}
