use crate::http::ui::models::errors::AppError;
use crate::http::ui::models::properties::{
    Properties, Property, TableSettingsResponse, TableSnapshotsResponse,
    TableUpdatePropertiesPayload,
};
use crate::http::ui::models::table::{
    Table, TableCreatePayload, TableQueryRequest, TableQueryResponse, TableRegisterRequest,
    TableUploadPayload
};
use crate::http::utils::get_default_properties;
use crate::state::AppState;
use axum::{extract::Multipart, extract::Path, extract::State, Json};
use catalog::models::{DatabaseIdent, TableIdent, WarehouseIdent};
use iceberg::NamespaceIdent;
use std::time::Instant;
use utoipa::OpenApi;
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_table,
        register_table,
        delete_table,
        get_table,
        query_table,
        upload_data_to_table,
        get_settings,
        update_table_properties,
        get_snapshots,
    ),
    components(
        schemas(
            TableQueryResponse,
            TableQueryRequest,
            TableCreatePayload,
            TableRegisterRequest,
            TableUploadPayload,
            Table,
            Properties,
            Property,
            AppError,
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
        (status = 404, description = "Not found", body = AppError),
        (status = 422, description = "Unprocessable entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn get_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> Result<Json<Table>, AppError> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split(".")
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
                .unwrap(),
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
        (status = 404, description = "Not found", body = AppError),
    )
)]
pub async fn create_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
    Json(payload): Json<TableCreatePayload>,
) -> Result<Json<Table>, AppError> {
    let warehouse = state.get_warehouse_model(warehouse_id).await?;
    let profile = state
        .control_svc
        .get_profile(warehouse.storage_profile_id)
        .await?;
    let db_ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::from_vec(
            database_name
                .split(".")
                .map(String::from)
                .collect::<Vec<String>>(),
        )
            .unwrap(),
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
        .map_err(|e| {
            let fmt = format!("{}: failed to create table", e);
            AppError::new(e, fmt.as_str())
        })?;
    let mut table: Table = table.into();
    table.with_details(warehouse_id, profile.into(), database_name);
    Ok(Json(table.into()))
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
        (status = 404, description = "Not found", body = AppError),
    )
)]
pub async fn register_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
    Json(payload): Json<TableRegisterRequest>,
) -> Result<Json<Table>, AppError> {
    let warehouse = state.get_warehouse_model(warehouse_id).await?;
    let profile = state
        .control_svc
        .get_profile(warehouse.storage_profile_id)
        .await?;
    let db_ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::from_vec(
            database_name
                .split(".")
                .map(String::from)
                .collect::<Vec<String>>(),
        )
            .unwrap(),
    };
    let table = state
        .catalog_svc
        .register_table(
            &db_ident,
            &profile,
            &warehouse,
            payload.name,
            payload.metadata_location,
            Option::from(get_default_properties()),
        )
        .await
        .map_err(|e| {
            let fmt = format!("{}: failed to register table", e);
            AppError::new(e, fmt.as_str())
        })?;
    let mut table: Table = table.into();
    table.with_details(warehouse_id, profile.into(), database_name);
    Ok(Json(table.into()))
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
        (status = 404, description = "Not found", body=AppError),
    )
)]
pub async fn delete_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> Result<(), AppError> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split(".")
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
                .unwrap(),
        },
        table: table_name,
    };
    state
        .catalog_svc
        .drop_table(&table_ident)
        .await
        .map_err(|e| {
            let fmt = format!("{}: failed to delete table with ident {}", e, &table_ident);
            AppError::new(e, fmt.as_str())
        })?;
    Ok(())
}

#[utoipa::path(
    post,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}/query",
    request_body = TableQueryRequest,
    operation_id = "tableQuery",
    tags = ["tables"],
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID"),
        ("databaseName" = Uuid, Path, description = "Database Name"),
        ("tableName" = Uuid, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Returns result of the query", body = TableQueryResponse),
        (status = 422, description = "Unprocessable entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
// Add time sql took
pub async fn query_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
    Json(payload): Json<TableQueryRequest>,
) -> Result<Json<TableQueryResponse>, AppError> {
    let request: TableQueryRequest = payload.into();
    let start = Instant::now();
    let result = state
        .control_svc
        .query_table(&warehouse_id, &database_name, &table_name, &request.query)
        .await
        .map_err(|e| {
            let fmt = format!("{}", e);
            AppError::new(e, fmt.as_str())
        })?;
    let duration = start.elapsed();
    Ok(Json(TableQueryResponse {
        query: request.query.clone(),
        result: result.to_string(),
        duration_seconds: duration.as_secs_f32(),
    }))
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
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn get_settings(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> Result<Json<TableSettingsResponse>, AppError> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split(".")
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
                .unwrap(),
        },
        table: table_name,
    };
    let mut table = state.get_table(&table_ident).await?;
    table.with_details(warehouse_id, profile, database_name);
    Ok(Json(table.into()))
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
        (status = 404, description = "Not found", body = AppError),
        (status = 422, description = "Unprocessable entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn update_table_properties(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
    Json(payload): Json<TableUpdatePropertiesPayload>,
) -> Result<Json<Table>, AppError> {
    let warehouse = state
        .control_svc
        .get_warehouse(warehouse_id)
        .await
        .map_err(|e| {
            let fmt = format!("{}: failed to get warehouse by id {}", e, warehouse_id);
            AppError::new(e, fmt.as_str())
        })?;
    let profile = state
        .control_svc
        .get_profile(warehouse.storage_profile_id)
        .await
        .map_err(|e| {
            let fmt = format!(
                "{}: failed to get profile by id {}",
                e, warehouse.storage_profile_id
            );
            AppError::new(e, fmt.as_str())
        })?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split(".")
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
                .unwrap(),
        },
        table: table_name,
    };
    let table = state.get_table(&table_ident).await?;
    let updated_table = state
        .catalog_svc
        .update_table(&profile, &warehouse, payload.to_commit(table, &table_ident))
        .await
        .map_err(|e| {
            let fmt = format!("{}: failed to update table properties", e);
            AppError::new(e, fmt.as_str())
        })?;
    let mut table: Table = updated_table.into();
    table.with_details(warehouse_id, profile.into(), database_name);
    Ok(Json(table.into()))
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
        (status = 200, description = "Returns result of the query", body = TableQueryResponse),
        (status = 422, description = "Unprocessable entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn upload_data_to_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
    mut multipart: Multipart,
) -> Result<(), AppError> {
    while let Some(field) = multipart
        .next_field()
        .await
        .expect("Failed to get next field!")
    {
        if field.name().unwrap() != "upload_file" {
            continue;
        }
        let file_name = field.file_name().unwrap().to_string();
        let data = field.bytes().await.unwrap();

        let _result = state
            .control_svc
            .upload_data_to_table(&warehouse_id, &database_name, &table_name, data, file_name)
            .await
            .map_err(|e| {
                let fmt = format!("{}", e);
                AppError::new(e, fmt.as_str())
            })?;
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
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn get_snapshots(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> Result<Json<TableSnapshotsResponse>, AppError> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::from_vec(
                database_name
                    .split(".")
                    .map(String::from)
                    .collect::<Vec<String>>(),
            )
                .unwrap(),
        },
        table: table_name,
    };
    let mut table = state.get_table(&table_ident).await?;
    table.with_details(warehouse_id, profile, database_name);
    Ok(Json(table.into()))
}
