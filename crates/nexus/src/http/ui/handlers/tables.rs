use std::time::Instant;
use crate::http::ui::models::errors::AppError;
use crate::http::ui::models::table::{
    Table, TableCreatePayload, TableQueryRequest, TableQueryResponse,
};
use crate::state::AppState;
use axum::{extract::Path, extract::State, extract::Multipart, Json};
use catalog::models::{DatabaseIdent, TableIdent, WarehouseIdent};
use iceberg::NamespaceIdent;
use utoipa::OpenApi;
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_table,
        delete_table,
        get_table,
        query_table,
    ),
    components(
        schemas(
            TableQueryResponse,
            TableQueryRequest,
            TableCreatePayload,
            Table,
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
    let profile = state.get_profile_by_id(warehouse.storage_profile_id).await?;
    let table_ident = TableIdent {
        database: DatabaseIdent {
            warehouse: WarehouseIdent::new(warehouse.id),
            namespace: NamespaceIdent::new(database_name.clone()),
        },
        table: table_name,
    };
    let mut table = state.get_table(&table_ident).await?;
    table.storage_profile = profile;
    table.warehouse_id = warehouse_id;
    table.database_name = database_name;
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
    let profile = state.control_svc.get_profile(warehouse.storage_profile_id).await?;
    let db_ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::new(database_name),
    };

    let table = state
        .catalog_svc
        .create_table(&db_ident, &profile, &warehouse, payload.into())
        .await
        .map_err(|e| {
            let fmt = format!("{}: failed to create table", e);
            AppError::new(e, fmt.as_str())
        })?;
    let mut table: Table = table.into();
    table.storage_profile = profile.into();
    table.warehouse_id = warehouse_id;
    Ok(Json(table.into()))
}

#[utoipa::path(
    delete,
    operation_id = "deleteTable",
    tags = ["tables"],
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID"),
        ("databaseName" = Uuid, Path, description = "Database Name"),
        ("tableName" = Uuid, Path, description = "Table name")
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
            namespace: NamespaceIdent::new(database_name),
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
    // request_body = TableQueryRequest,
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
        duration_seconds: duration.as_secs_f32()
    }))
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
    responses(
        (status = 200, description = "Returns result of the query", body = TableQueryResponse),
        (status = 422, description = "Unprocessable entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn upload_data_to_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
    mut multipart: Multipart
) -> Result<(), AppError> {
    while let Some(field) = multipart
        .next_field().await.expect("Failed to get next field!")
    {
        if field.name().unwrap() != "upload_file" {
            continue;
        }
        let file_name = field.file_name().unwrap().to_string();
        let data = field.bytes().await.unwrap();

        let result = state.control_svc
            .upload_data_to_table(&warehouse_id, &database_name, &table_name, data, file_name)
            .await.map_err(|e| {
            let fmt = format!("{}", e);
            AppError::new(e, fmt.as_str())
        })?;
    }
    Ok(())
}
