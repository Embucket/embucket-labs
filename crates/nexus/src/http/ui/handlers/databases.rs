use crate::http::ui::models::database::{CreateDatabasePayload, Database};
use crate::http::ui::models::errors::AppError;
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use catalog::models::{DatabaseIdent, WarehouseIdent};
use iceberg::NamespaceIdent;
use swagger;
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
            AppError,
        )
    ),
    tags(
        (name = "Databases", description = "Databases management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    path = "/ui/warehouses/{warehouseId}/databases",
    operation_id = "webCreateDatabase",
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
    ),
    request_body = CreateDatabasePayload,
    responses(
        (status = 200, description = "Successful Response", body = Database),
        (status = 400, description = "Bad request", body = AppError),
        (status = 422, description = "Unprocessable entity", body = AppError),
        (status = 500, description = "Internal server error", body = AppError)
    )
)]
pub async fn create_database(
    State(state): State<AppState>,
    Path(warehouse_id): Path<Uuid>,
    Json(payload): Json<CreateDatabasePayload>,
) -> Result<Json<Database>, AppError> {
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::new(payload.name),
    };
    let res = state
        .catalog_svc
        .create_namespace(&ident, payload.properties.unwrap_or_default())
        .await
        .map_err(|e| {
            let fmt = format!("{}: failed to create database with ident {}", e, &ident);
            AppError::new(e, fmt.as_str())
        })?;
    Ok(Json(res.into()))
}

#[utoipa::path(
    delete,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}",
    operation_id = "webDeleteDatabase",
    params(
        ("warehouseId" = Uuid, description = "Warehouse ID"),
        ("databaseName" = String, description = "Database Name"),
    ),
    responses(
        (status = 204, description = "Successful Response"),
        (status = 404, description = "Database not found", body = AppError),
        (status = 422, description = "Unprocessable entity", body = AppError),
    )
)]
pub async fn delete_database(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
) -> Result<Json<()>, AppError> {
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse_id),
        namespace: NamespaceIdent::new(database_name),
    };

    state
        .catalog_svc
        .drop_namespace(&ident)
        .await
        .map_err(|e| {
            let fmt = format!("{}: failed to delete database with ident {}", e, &ident);
            AppError::new(e, fmt.as_str())
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
    operation_id = "webDatabaseDashboard",
    responses(
        (status = 200, description = "Successful Response", body = Database),
        (status = 404, description = "Database not found", body = AppError),
        (status = 422, description = "Unprocessable entity", body = AppError),
    )
)]
pub async fn get_database(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
) -> Result<Json<Database>, AppError> {
    let mut warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id.unwrap())
        .await?;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::new(database_name),
    };
    let mut database = state.get_database(&ident).await?;
    let tables = state.list_tables(&ident).await?;

    warehouse.with_details(Option::from(profile), None);
    database.with_details(Option::from(tables));
    Ok(Json(database))
}
