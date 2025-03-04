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
use crate::http::ui::models::database::{CreateDatabasePayload, Database};
use crate::http::utils::update_properties_timestamps;
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use catalog::models::{DatabaseIdent, WarehouseIdent};
use iceberg::NamespaceIdent;
use snafu::ResultExt;
use utoipa::OpenApi;
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_database,
        delete_database,
        get_database,
    ),
    components(
        schemas(
            CreateDatabasePayload,
            Database,
            NexusError,
        )
    ),
    tags(
        (name = "databases", description = "Databases management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    path = "/ui/warehouses/{warehouseId}/databases",
    operation_id = "createDatabase",
    tags = ["databases"],
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
    ),
    request_body = CreateDatabasePayload,
    responses(
        (status = 200, description = "Successful Response", body = Database),
        (status = 400, description = "Bad request", body = NexusError),
        (status = 422, description = "Unprocessable entity", body = NexusError),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_database(
    State(state): State<AppState>,
    Path(warehouse_id): Path<Uuid>,
    Json(payload): Json<CreateDatabasePayload>,
) -> NexusResult<Json<Database>> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let databases = state.list_databases_models(warehouse_id).await?;
    let name = payload.name;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::from_vec(
            name.split('.').map(String::from).collect::<Vec<String>>(),
        )
        .context(model_error::MalformedNamespaceIdentSnafu)?,
    };

    if databases
        .iter()
        .any(|db| db.ident.namespace == ident.namespace)
    {
        return Err(NexusError::DatabaseAlreadyExists { name });
    }

    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;

    let mut properties = payload.properties.unwrap_or_default();
    update_properties_timestamps(&mut properties);

    let database = state
        .catalog_svc
        .create_namespace(&ident, properties)
        .await
        .context(model_error::DatabaseCreateSnafu {
            ident: ident.clone(),
        })?;
    let mut database: Database = database.into();
    database.with_details(warehouse_id, &profile, vec![]);

    Ok(Json(database))
}

#[utoipa::path(
    delete,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}",
    operation_id = "deleteDatabase",
    tags = ["databases"],
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
    ),
    responses(
        (status = 204, description = "Successful Response"),
        (status = 404, description = "Database not found", body = NexusError),
        (status = 422, description = "Unprocessable entity", body = NexusError),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_database(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
) -> NexusResult<Json<()>> {
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse_id),
        namespace: NamespaceIdent::from_vec(
            database_name
                .split('.')
                .map(String::from)
                .collect::<Vec<String>>(),
        )
        .context(model_error::MalformedNamespaceIdentSnafu)?,
    };

    state
        .catalog_svc
        .drop_namespace(&ident)
        .await
        .context(model_error::DatabaseDeleteSnafu {
            ident: ident.clone(),
        })?;
    Ok(Json(()))
}

#[utoipa::path(
    get,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}",
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
    ),
    operation_id = "getDatabase",
    tags = ["databases"],
    responses(
        (status = 200, description = "Successful Response", body = Database),
        (status = 404, description = "Database not found", body = NexusError),
        (status = 422, description = "Unprocessable entity", body = NexusError),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_database(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
) -> NexusResult<Json<Database>> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::from_vec(
            database_name
                .split('.')
                .map(String::from)
                .collect::<Vec<String>>(),
        )
        .context(model_error::MalformedNamespaceIdentSnafu)?,
    };
    let mut database = state.get_database(&ident).await?;
    let tables = state.list_tables(&ident).await?;

    database.with_details(warehouse_id, &profile, tables);
    Ok(Json(database))
}
