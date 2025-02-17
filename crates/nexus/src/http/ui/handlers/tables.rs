use super::super::models::error::{self as model_error, NexusError, NexusResult};
use crate::http::ui::models::properties::{
    Properties, Property, TableSettingsResponse, TableSnapshotsResponse,
    TableUpdatePropertiesPayload,
};
use crate::http::ui::models::table::{
    Table, TableCreatePayload, TableRegisterRequest, TableUploadPayload,
};
use crate::http::{session::DFSessionId, utils::get_default_properties};
use crate::state::AppState;
use axum::{extract::Multipart, extract::Path, extract::State, Json};
use catalog::models::{DatabaseIdent, TableIdent, WarehouseIdent};
use iceberg::NamespaceIdent;
use snafu::ResultExt;
use utoipa::OpenApi;
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_table,
        register_table,
        delete_table,
        get_table,
        upload_data_to_table,
        get_settings,
        update_table_properties,
        get_snapshots,
    ),
    components(
        schemas(
            TableCreatePayload,
            TableRegisterRequest,
            TableUploadPayload,
            Table,
            Properties,
            Property,
            NexusError,
        )
    ),
    tags(
        (name = "tables", description = "Tables management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}",
    operation_id = "getTable",
    tags = ["tables"],
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
        ("tableName" = String, description = "Table name")
    ),
    responses(
        (status = 200, description = "Get table", body = Table),
        (status = 404, description = "Not found", body = NexusError),
        (status = 422, description = "Unprocessable entity", body = NexusError),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> NexusResult<Json<Table>> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split('.')
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
            .context(model_error::MalformedNamespaceIdentSnafu)?,
        },
        table: table_name,
    };
    let mut table = state.get_table(&table_ident).await?;
    table.with_details(warehouse_id, profile, database_name);
    Ok(Json(table))
}

#[utoipa::path(
    get,
    operation_id = "createTable",
    tags = ["tables"],
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables",
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
    ),
    responses(
        (status = 200, description = "Successful Response", body = Table),
        (status = 404, description = "Not found", body = NexusError),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
    Json(payload): Json<TableCreatePayload>,
) -> NexusResult<Json<Table>> {
    let warehouse = state.get_warehouse_model(warehouse_id).await?;
    let profile = state
        .control_svc
        .get_profile(warehouse.storage_profile_id)
        .await
        .context(model_error::StorageProfileFetchSnafu {
            id: warehouse.storage_profile_id,
        })?;
    let db_ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::from_vec(
            database_name
                .split('.')
                .map(String::from)
                .collect::<Vec<String>>(),
        )
        .context(model_error::MalformedNamespaceIdentSnafu)?,
    };
    let table = state
        .catalog_svc
        .create_table(
            &db_ident,
            &profile,
            &warehouse,
            payload.into(),
            Option::from(get_default_properties()),
        )
        .await
        .context(model_error::TableCreateSnafu)?;
    let mut table: Table = table.into();
    table.with_details(warehouse_id, profile.into(), database_name);
    Ok(Json(table))
}

#[utoipa::path(
    get,
    operation_id = "registerTable",
    tags = ["tables"],
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/register",
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
    ),
    responses(
        (status = 200, description = "Successful Response", body = Table),
        (status = 404, description = "Not found", body = NexusError),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn register_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
    Json(payload): Json<TableRegisterRequest>,
) -> NexusResult<Json<Table>> {
    let warehouse = state.get_warehouse_model(warehouse_id).await?;
    let profile = state
        .control_svc
        .get_profile(warehouse.storage_profile_id)
        .await
        .context(model_error::StorageProfileFetchSnafu {
            id: warehouse.storage_profile_id,
        })?;
    let db_ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::from_vec(
            database_name
                .split('.')
                .map(String::from)
                .collect::<Vec<String>>(),
        )
        .context(model_error::MalformedNamespaceIdentSnafu)?,
    };
    let table = state
        .catalog_svc
        .register_table(
            &db_ident,
            &profile,
            payload.name,
            payload.metadata_location,
            Option::from(get_default_properties()),
        )
        .await
        .context(model_error::TableRegisterSnafu)?;
    let mut table: Table = table.into();
    table.with_details(warehouse_id, profile.into(), database_name);
    Ok(Json(table))
}

#[utoipa::path(
    delete,
    operation_id = "deleteTable",
    tags = ["tables"],
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}",
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = Uuid, description = "Database Name"),
        ("tableName" = Uuid, description = "Table name")
    ),
    responses(
        (status = 200, description = "Successful Response"),
        (status = 404, description = "Not found", body=NexusError),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> NexusResult<()> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split('.')
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
            .context(model_error::MalformedNamespaceIdentSnafu)?,
        },
        table: table_name,
    };
    state
        .catalog_svc
        .drop_table(&table_ident)
        .await
        .context(model_error::TableDeleteSnafu)?;
    Ok(())
}

#[utoipa::path(
    get,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}/settings",
    operation_id = "getTableSettings",
    tags = ["tables"],
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
        ("tableName" = String, description = "Table name")
    ),
    responses(
        (status = 200, description = "Get table", body = TableSettingsResponse),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_settings(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> NexusResult<Json<TableSettingsResponse>> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split('.')
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
            .context(model_error::MalformedNamespaceIdentSnafu)?,
        },
        table: table_name,
    };
    let mut table = state.get_table(&table_ident).await?;
    table.with_details(warehouse_id, profile, database_name);
    Ok(Json(table.try_into()?))
}

#[utoipa::path(
    post,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}/settings",
    operation_id = "updateTableProperties",
    tags = ["tables"],
    request_body = TableUpdatePropertiesPayload,
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
        ("tableName" = String, description = "Table name")
    ),
    responses(
        (status = 200, description = "Get table", body = Table),
        (status = 404, description = "Not found", body = NexusError),
        (status = 422, description = "Unprocessable entity", body = NexusError),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_table_properties(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
    Json(payload): Json<TableUpdatePropertiesPayload>,
) -> NexusResult<Json<Table>> {
    let warehouse = state
        .control_svc
        .get_warehouse(warehouse_id)
        .await
        .context(model_error::WarehouseFetchSnafu { id: warehouse_id })?;
    let profile = state
        .control_svc
        .get_profile(warehouse.storage_profile_id)
        .await
        .context(model_error::StorageProfileFetchSnafu {
            id: warehouse.storage_profile_id,
        })?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split('.')
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
            .context(model_error::MalformedNamespaceIdentSnafu)?,
        },
        table: table_name,
    };
    let table = state.get_table(&table_ident).await?;
    let updated_table = state
        .catalog_svc
        .update_table(
            &profile,
            &warehouse,
            payload.to_commit(&table, &table_ident),
        )
        .await
        .context(model_error::TablePropertiesUpdateSnafu)?;
    let mut table: Table = updated_table.into();
    table.with_details(warehouse_id, profile.into(), database_name);
    Ok(Json(table))
}

#[utoipa::path(
    post,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}/upload",
    operation_id = "tableUpload",
    tags = ["tables"],
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID"),
        ("databaseName" = Uuid, Path, description = "Database Name"),
        ("tableName" = Uuid, Path, description = "Table name")
    ),
    request_body(
        content = TableUploadPayload,
        content_type = "multipart/form-data",
        description = "Upload data to the table in multipart/form-data format"
    ),
    responses(
        (status = 200, description = "Successful Response"),
        (status = 422, description = "Unprocessable entity", body = NexusError),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state, multipart), err, ret(level = tracing::Level::TRACE))]
pub async fn upload_data_to_table(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
    mut multipart: Multipart,
) -> NexusResult<()> {
    loop {
        let next_field = multipart
            .next_field()
            .await
            .context(model_error::MalformedMultipartSnafu)?;
        match next_field {
            Some(field) => {
                if field.name().ok_or(NexusError::MalformedFileUploadRequest)? != "uploadFile" {
                    continue;
                }
                let file_name = field
                    .file_name()
                    .ok_or(NexusError::MalformedFileUploadRequest)?
                    .to_string();
                let data = field
                    .bytes()
                    .await
                    .context(model_error::MalformedMultipartSnafu)?;

                state
                    .control_svc
                    .upload_data_to_table(
                        &session_id,
                        &warehouse_id,
                        &database_name,
                        &table_name,
                        data,
                        file_name,
                    )
                    .await
                    .context(model_error::DataUploadSnafu)?;
            }
            None => {
                break;
            }
        }
    }
    Ok(())
}

#[utoipa::path(
    get,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}/snapshots",
    operation_id = "getTableSnapshots",
    tags = ["tables"],
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
        ("tableName" = String, description = "Table name")
    ),
    responses(
        (status = 200, description = "Get table", body = TableSnapshotsResponse),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_snapshots(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> NexusResult<Json<TableSnapshotsResponse>> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split('.')
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
            .context(model_error::MalformedNamespaceIdentSnafu)?,
        },
        table: table_name,
    };
    let mut table = state.get_table(&table_ident).await?;
    table.with_details(warehouse_id, profile, database_name);
    Ok(Json(table.try_into()?))
}
