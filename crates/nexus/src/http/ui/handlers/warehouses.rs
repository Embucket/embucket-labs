use crate::http::ui::models::database::{get_database_id, DatabaseShort};
use crate::http::ui::models::errors::AppError;
use crate::http::ui::models::table::{get_table_id, TableShort};
use crate::http::ui::models::warehouse;
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use catalog::models::WarehouseIdent;
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
            warehouse::CreateWarehousePayload,
            warehouse::Warehouse,
            warehouse::WarehouseExtended,
            warehouse::WarehousesDashboard,
            warehouse::WarehouseEntity,
            warehouse::WarehouseShort,
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
        (status = 200, description = "List all warehouses fot navigation", body = warehouse::Navigation),
    )
)]
pub async fn navigation(
    State(state): State<AppState>,
) -> Result<Json<warehouse::Navigation>, AppError> {
    let warehouses = state.control_svc.list_warehouses().await.map_err(|e| {
        let fmt = format!("{}: failed to get warehouses", e);
        AppError::new(e, fmt.as_str())
    })?;
    let mut warehouses_short = Vec::new();

    for warehouse in warehouses {
        let databases = state
            .catalog_svc
            .list_namespaces(&WarehouseIdent::new(warehouse.id), None)
            .await
            .map_err(|e| {
                let fmt = format!(
                    "{}: failed to get warehouse databases with wh id {}",
                    e, warehouse.id
                );
                AppError::new(e, fmt.as_str())
            })?;
        let mut databases_short = Vec::new();

        for database in databases {
            let tables = state.catalog_svc.list_tables(&database.ident).await?;
            let ident = database.ident.clone();
            databases_short.push(DatabaseShort {
                id: get_database_id(database.ident),
                name: ident.to_string(),
                tables: tables
                    .into_iter()
                    .map(|t| {
                        let ident = t.ident.clone();
                        TableShort {
                            id: get_table_id(t.ident),
                            name: ident.table,
                        }
                    })
                    .collect(),
            });
        }
        warehouses_short.push(warehouse::WarehouseShort {
            id: warehouse.id,
            name: warehouse.name,
            databases: databases_short,
        });
    }
    Ok(Json(warehouse::Navigation {
        warehouses: warehouses_short,
    }))
}
#[utoipa::path(
    get,
    path = "/ui/warehouses",
    operation_id = "webWarehousesDashboard",
    responses(
        (status = 200, description = "List all warehouses", body = warehouse::WarehousesDashboard),
        (status = 500, description = "List all warehouses error", body = AppError),

    )
)]
pub async fn list_warehouses(
    State(state): State<AppState>,
) -> Result<Json<warehouse::WarehousesDashboard>, AppError> {
    let warehouses = state.control_svc.list_warehouses().await.map_err(|e| {
        let fmt = format!("{}: failed to get warehouses", e);
        AppError::new(e, fmt.as_str())
    })?;
    let storage_profiles = state.control_svc.list_profiles().await.map_err(|e| {
        let fmt = format!("{}: failed to get profiles", e);
        AppError::new(e, fmt.as_str())
    })?;
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
        (status = 200, description = "Warehouse found", body = warehouse::WarehouseDashboard),
        (status = 404, description = "Warehouse not found", body = AppError)
    )
)]
pub async fn get_warehouse(
    State(state): State<AppState>,
    Path(warehouse_id): Path<Uuid>,
) -> Result<Json<warehouse::WarehouseDashboard>, AppError> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let databases = state
        .catalog_svc
        .list_namespaces(&WarehouseIdent::new(warehouse.id), None)
        .await
        .map_err(|e| {
            let fmt = format!(
                "{}: failed to get warehouse databases with wh id {}",
                e, warehouse.id
            );
            AppError::new(e, fmt.as_str())
        })?;
    let mut dashboard = warehouse::WarehouseDashboard::new(
        warehouse.into(),
        profile.into(),
        databases.into_iter().map(|d| d.into()).collect(),
    );

    if let Ok(profile) = state
        .control_svc
        .get_profile(dashboard.storage_profile_id)
        .await
    {
        dashboard.storage_profile = profile.into();
    }

    Ok(Json(dashboard))
}

#[utoipa::path(
    post,
    path = "/ui/warehouses",
    request_body = warehouse::CreateWarehousePayload,
    operation_id = "webCreateWarehouse",
    responses(
        (status = 201, description = "Warehouse created", body = warehouse::Warehouse),
        (status = 422, description = "Unprocessable Entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn create_warehouse(
    State(state): State<AppState>,
    Json(payload): Json<warehouse::CreateWarehousePayload>,
) -> Result<Json<warehouse::Warehouse>, AppError> {
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
    operation_id = "webCreateWarehouse",
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
