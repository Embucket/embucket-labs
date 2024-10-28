use crate::http::ui::models::errors::AppError;
use crate::http::ui::models::table;
use crate::http::ui::models::table::TableQueryRequest;
use crate::http::ui::models::table::{
    Table, TableCreateRequest, TableExtended, TableQueryResponse,
};
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
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
            TableExtended,
            TableCreateRequest,
            Table,
            AppError,
        )
    ),
    tags(
        (name = "Tables", description = "Tables management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}",
    operation_id = "webTableDashboard",
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
        ("tableName" = String, description = "Table name")
    ),
    responses(
        (status = 200, description = "List all warehouses"),
        (status = 404, description = "Not found", body = AppError),
        (status = 422, description = "Unprocessable entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn get_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> Result<Json<TableExtended>, AppError> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::new(database_name),
    };
    let database = state.get_database_by_ident(&ident).await?;
    let table_ident = TableIdent {
        database: ident,
        table: table_name,
    };
    let table = state
        .catalog_svc
        .load_table(&table_ident)
        .await
        .map_err(|e| {
            let fmt = format!("{}: failed to get table with ident {}", e, &table_ident);
            AppError::new(e, fmt.as_str())
        })?;

    Ok(Json(TableExtended::new(
        profile.into(),
        warehouse.into(),
        database.into(),
        table,
    )))
}

#[utoipa::path(
    get,
    operation_id = "webCreateTable",
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables",
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
    ),
    responses(
        (status = 200, description = "Successful Response"),
        (status = 404, description = "Not found"),
    )
)]
pub async fn create_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
    Json(payload): Json<TableCreateRequest>,
) -> Result<Json<Table>, AppError> {
    let warehouse = state.get_warehouse_model(warehouse_id).await?;
    let db_ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::new(database_name),
    };

    let table = state
        .catalog_svc
        .create_table(&db_ident, &warehouse, payload.into())
        .await
        .map_err(|e| {
            let fmt = format!("{}: failed to create table", e);
            AppError::new(e, fmt.as_str())
        })?;
    Ok(Json(table.into()))
}

#[utoipa::path(
    delete,
    operation_id = "webDeleteTable",
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
    request_body = TableQueryRequest,
    operation_id = "webTableQuery",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID"),
        ("databaseName" = Uuid, Path, description = "Database Name"),
        ("tableName" = Uuid, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Returns result of the query", body = TableQueryResponse),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn query_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
    Json(payload): Json<table::TableQueryRequest>,
) -> Result<Json<table::TableQueryResponse>, AppError> {
    let request: TableQueryRequest = payload.into();
    let result = state
        .control_svc
        .query_table(&warehouse_id, &request.query)
        .await?;
    Ok(Json(table::TableQueryResponse {
        id: Default::default(),
        query: request.query.clone(),
        result: result.to_string(),
    }))
}
