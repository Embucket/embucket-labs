// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::super::models::error::{self as model_error, NexusError, NexusResult};
use crate::http::ui::models::table::Statistics;
use crate::http::ui::models::warehouse::{
    CreateWarehousePayload, Navigation, Warehouse, WarehousesDashboard,
};
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use control_plane::models::{Warehouse as WarehouseModel, WarehouseCreateRequest};
use snafu::ResultExt;
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
            NexusError,
        )
    ),
    tags(
        (name = "warehouses", description = "Warehouse management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/navigation",
    tags = ["warehouses"],
    operation_id = "warehousesNavigation",
    responses(
        (status = 200, description = "List all warehouses fot navigation", body = Navigation),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn navigation(State(state): State<AppState>) -> NexusResult<Json<Navigation>> {
    let warehouses = state.list_warehouses().await?;
    Ok(Json(Navigation { warehouses }))
}
#[utoipa::path(
    get,
    path = "/ui/warehouses",
    tags = ["warehouses"],
    operation_id = "warehousesDashboard",
    responses(
        (status = 200, description = "List all warehouses", body = WarehousesDashboard),
        (status = 500, description = "List all warehouses error", body = NexusError),

    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_warehouses(
    State(state): State<AppState>,
) -> NexusResult<Json<WarehousesDashboard>> {
    let warehouses = state.list_warehouses().await?;

    let mut total_statistics = Statistics::default();
    let dashboards = warehouses
        .into_iter()
        .inspect(|warehouse| {
            total_statistics = total_statistics.aggregate(&warehouse.statistics);
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
    operation_id = "getWarehouse",
    tags = ["warehouses"],
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID")
    ),
    responses(
        (status = 200, description = "Warehouse found", body = Warehouse),
        (status = 404, description = "Warehouse not found", body = NexusError)
    )
)]
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::as_conversions
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_warehouse(
    State(state): State<AppState>,
    Path(warehouse_id): Path<Uuid>,
) -> NexusResult<Json<Warehouse>> {
    let mut warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let databases = state.list_databases(warehouse_id, profile.clone()).await?;

    let mut total_statistics = Statistics {
        database_count: Some(databases.len() as i32),
        ..Default::default()
    };
    for database in &databases {
        let stats = database.clone().statistics;
        total_statistics = total_statistics.aggregate(&stats);
    }
    warehouse.storage_profile = profile;
    warehouse.databases = databases;
    warehouse.statistics = total_statistics;
    Ok(Json(warehouse))
}

#[utoipa::path(
    post,
    path = "/ui/warehouses",
    request_body = CreateWarehousePayload,
    operation_id = "createWarehouse",
    tags = ["warehouses"],
    responses(
        (status = 201, description = "Warehouse created", body = Warehouse),
        (status = 422, description = "Unprocessable Entity", body = NexusError),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_warehouse(
    State(state): State<AppState>,
    Json(payload): Json<CreateWarehousePayload>,
) -> NexusResult<Json<Warehouse>> {
    let request: WarehouseCreateRequest = payload.into();
    let profile = state.get_profile_by_id(request.storage_profile_id).await?;
    let warehouses = state
        .control_svc
        .list_warehouses()
        .await
        .context(model_error::WarehouseListSnafu)?;

    if warehouses.iter().any(|w| w.name == request.name) {
        return Err(NexusError::WarehouseAlreadyExists {
            name: request.name.clone(),
        });
    }
    let warehouse: WarehouseModel = state
        .control_svc
        .create_warehouse(&request)
        .await
        .context(model_error::WarehouseCreateSnafu)?;
    let mut warehouse: Warehouse = warehouse.into();
    warehouse.storage_profile = profile;
    Ok(Json(warehouse))
}

#[utoipa::path(
    delete,
    path = "/ui/warehouses/{warehouseId}",
    operation_id = "deleteWarehouse",
    tags = ["warehouses"],
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID")
    ),
    responses(
        (status = 204, description = "Warehouse deleted"),
        (status = 404, description = "Warehouse not found", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_warehouse(
    State(state): State<AppState>,
    Path(warehouse_id): Path<Uuid>,
) -> NexusResult<Json<()>> {
    state
        .control_svc
        .delete_warehouse(warehouse_id)
        .await
        .context(model_error::WarehouseDeleteSnafu { id: warehouse_id })?;
    Ok(Json(()))
}
