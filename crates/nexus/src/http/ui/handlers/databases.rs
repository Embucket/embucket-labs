use crate::http::ui::models::database::{CreateDatabasePayload, Database};
use crate::http::ui::models::errors::AppError;
use crate::http::utils::update_properties_timestamps;
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use catalog::models::{DatabaseIdent, WarehouseIdent};
use iceberg::NamespaceIdent;
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
    let databases = state.list_databases_models(warehouse_id).await?;
    let name = payload.name;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::new(name.clone()),
    };

    if databases
        .iter()
        .any(|db| db.ident.namespace == ident.namespace)
    {
        return Err(AppError::AlreadyExists(format!("database with name {} already exists", name)));
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
        .map_err(|e| {
            let fmt = format!("{}: failed to create database with ident {}", e, &ident);
            AppError::new(e, fmt.as_str())
        })?;
    let mut database: Database = database.into();
    database.with_details(warehouse_id, profile, vec![]);

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
    operation_id = "getDatabase",
    tags = ["databases"],
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
    let warehouse = state.get_warehouse_by_id(warehouse_id).await?;
    let profile = state
        .get_profile_by_id(warehouse.storage_profile_id)
        .await?;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(warehouse.id),
        namespace: NamespaceIdent::new(database_name),
    };
    let mut database = state.get_database(&ident).await?;
    let tables = state.list_tables(&ident).await?;

    database.with_details(warehouse_id, profile, tables);
    Ok(Json(database))
}
