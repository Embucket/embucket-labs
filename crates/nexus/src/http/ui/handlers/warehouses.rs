use crate::http::ui::models::errors::AppError;
use crate::http::ui::models::table::Statistics;
use crate::http::ui::models::warehouse::{
    CreateWarehousePayload, Navigation, Warehouse, WarehousesDashboard,
};
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use control_plane::models::{Warehouse as WarehouseModel, WarehouseCreateRequest};
use utoipa::OpenApi;
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(
        navigation,
        get_warehouse,
        list_warehouses,
        create_warehouse,
        delete_warehouse,
    ),
    components(
        schemas(
            CreateWarehousePayload,
            Warehouse,
            Navigation,
            AppError,
        )
    ),
    tags(
        (name = "Warehouse", description = "Warehouse management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/navigation",
    operation_id = "webWarehousesNavigation",
    responses(
        (status = 200, description = "List all warehouses fot navigation", body = Navigation),
    )
)]
pub async fn navigation(State(state): State<AppState>) -> Result<Json<Navigation>, AppError> {
    let warehouses = state.list_warehouses().await?;
    Ok(Json(Navigation {
        warehouses,
    }))
}
#[utoipa::path(
    get,
    path = "/ui/warehouses",
    operation_id = "webWarehousesDashboard",
    responses(
        (status = 200, description = "List all warehouses", body = WarehousesDashboard),
        (status = 500, description = "List all warehouses error", body = AppError),

    )
)]
pub async fn list_warehouses(
    State(state): State<AppState>,
) -> Result<Json<WarehousesDashboard>, AppError> {
    let warehouses = state.list_warehouses().await?;

    let mut total_statistics = Statistics::default();
    let dashboards = warehouses
        .into_iter()
        .map(|warehouse| {
            if let Some(stats) = &warehouse.statistics {
                total_statistics = total_statistics.aggregate(stats);
            }
            warehouse.into()
        })
        .collect();

    Ok(Json(WarehousesDashboard {
        warehouses: dashboards,
        statistics: total_statistics,
        compaction_summary: None,
    }))
}

#[utoipa::path(
    get,
    path = "/ui/warehouses/{warehouseId}",
    operation_id = "webGetWarehouse",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID")
    ),
    responses(
        (status = 200, description = "Warehouse found", body = Warehouse),
        (status = 404, description = "Warehouse not found", body = AppError)
    )
)]
pub async fn get_warehouse(
    State(state): State<AppState>,
    Path(warehouse_id): Path<Uuid>,
) -> Result<Json<Warehouse>, AppError> {
    let mut warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id.unwrap())
        .await?;
    let databases = state.list_databases(warehouse_id, profile.clone()).await?;
    warehouse.with_details(Option::from(profile), Option::from(databases));
    Ok(Json(warehouse))
}

#[utoipa::path(
    post,
    path = "/ui/warehouses",
    request_body = CreateWarehousePayload,
    operation_id = "webCreateWarehouse",
    responses(
        (status = 201, description = "Warehouse created", body = Warehouse),
        (status = 422, description = "Unprocessable Entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn create_warehouse(
    State(state): State<AppState>,
    Json(payload): Json<CreateWarehousePayload>,
) -> Result<Json<Warehouse>, AppError> {
    let request: WarehouseCreateRequest = payload.into();
    state.get_profile_by_id(request.storage_profile_id).await?;
    let warehouse: WarehouseModel =
        state
            .control_svc
            .create_warehouse(&request)
            .await
            .map_err(|e| {
                let fmt = format!(
                    "{}: failed to get create warehouse with name {}",
                    e, request.name
                );
                AppError::new(e, fmt.as_str())
            })?;
    Ok(Json(warehouse.into()))
}

#[utoipa::path(
    delete,
    path = "/ui/warehouses/{warehouseId}",
    operation_id = "webDeleteWarehouse",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID")
    ),
    responses(
        (status = 204, description = "Warehouse deleted"),
        (status = 404, description = "Warehouse not found", body = AppError)
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
        .map_err(|e| {
            let fmt = format!(
                "{}: failed to get delete warehouse with id {}",
                e, warehouse_id
            );
            AppError::new(e, fmt.as_str())
        })?;
    Ok(Json(()))
}
