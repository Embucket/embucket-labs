use crate::error::AppError;
use crate::http::ui::models::{aws, database, storage_profile, table, warehouse};
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
use utoipa::OpenApi;
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_table,
        delete_table,
        get_table,
        list_tables,
    ),
    components(
        schemas(
            table::TableExtended,
            database::Database,
        )
    ),
    tags(
        (name = "Tables", description = "Tables management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables",
    operation_id = "webTablesDashboard",
    responses(
        (status = 200, description = "List all warehouses", body = Vec<table::TableExtended>),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn list_tables(
    State(state): State<AppState>,
) -> Result<Json<Vec<table::TableExtended>>, AppError> {
    Ok(Json(vec![]))
}

#[utoipa::path(
    get,
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}",
    operation_id = "webTableDashboard",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID"),
        ("databaseName" = Uuid, Path, description = "Database Name"),
        ("tableName" = Uuid, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "List all warehouses", body = Vec<table::TableExtended>),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn get_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> Result<Json<table::TableExtended>, AppError> {
    Ok(Json(table::TableExtended {
        id: Default::default(),
        name: table_name,
        database_name: Default::default(),
        warehouse_id: Default::default(),
        properties: None,
        metadata: Default::default(),
        statistics: None,
        compaction_summary: None,
        created_at: Default::default(),
        updated_at: Default::default(),
        database: database::DatabaseExtended {
            name: "1".to_string(),
            properties: None,
            id: Default::default(),
            warehouse_id,
            statistics: None,
            compaction_summary: None,
            created_at: Default::default(),
            updated_at: Default::default(),
            warehouse: warehouse::WarehouseExtended {
                name: "11".to_string(),
                storage_profile_id: Default::default(),
                key_prefix: "".to_string(),
                id: Default::default(),
                external_id: Default::default(),
                location: "".to_string(),
                created_at: Default::default(),
                updated_at: Default::default(),
                statistics: None,
                compaction_summary: None,
                storage_profile: storage_profile::StorageProfile {
                    r#type: aws::CloudProvider::S3,
                    region: "22".to_string(),
                    bucket: "2".to_string(),
                    credentials: Default::default(),
                    sts_role_arn: None,
                    endpoint: None,
                    id: Default::default(),
                    created_at: Default::default(),
                    updated_at: Default::default(),
                },
            },
        },
    }))
}

#[utoipa::path(
    get,
    operation_id = "webCreateTable",
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID"),
        ("databaseName" = Uuid, Path, description = "Database Name"),
        ("tableName" = Uuid, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Successful Response", body = table::TableExtended),
        (status = 404, description = "Not found"),
    )
)]
pub async fn create_table(
    State(state): State<AppState>,
    Json(payload): Json<table::TableExtended>,
) -> Result<Json<table::TableExtended>, AppError> {
    Ok(Json(table::TableExtended {
        id: Default::default(),
        name: "3".to_string(),
        database_name: "3".to_string(),
        warehouse_id: Default::default(),
        properties: None,
        metadata: Default::default(),
        statistics: None,
        compaction_summary: None,
        created_at: Default::default(),
        updated_at: Default::default(),
        database: database::DatabaseExtended {
            name: "4".to_string(),
            properties: None,
            id: Default::default(),
            warehouse_id: Default::default(),
            statistics: None,
            compaction_summary: None,
            created_at: Default::default(),
            updated_at: Default::default(),
            warehouse: warehouse::WarehouseExtended {
                name: "1".to_string(),
                storage_profile_id: Default::default(),
                key_prefix: "".to_string(),
                id: Default::default(),
                external_id: Default::default(),
                location: "".to_string(),
                created_at: Default::default(),
                updated_at: Default::default(),
                statistics: None,
                compaction_summary: None,
                storage_profile: storage_profile::StorageProfile {
                    r#type: aws::CloudProvider::S3,
                    region: "2".to_string(),
                    bucket: "2".to_string(),
                    credentials: Default::default(),
                    sts_role_arn: None,
                    endpoint: None,
                    id: Default::default(),
                    created_at: Default::default(),
                    updated_at: Default::default(),
                },
            },
        },
    }))
}

#[utoipa::path(
    get,
    operation_id = "webDeleteTable",
    path = "/ui/warehouses/{warehouseId}/databases/{databaseName}/tables/{tableName}",
    params(
        ("warehouseId" = Uuid, Path, description = "Warehouse ID"),
        ("databaseName" = Uuid, Path, description = "Database Name"),
        ("tableName" = Uuid, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Successful Response", body = table::TableExtended),
        (status = 404, description = "Not found"),
    )
)]
pub async fn delete_table(
    State(state): State<AppState>,
    Path((warehouse_id, database_name, table_name)): Path<(Uuid, String, String)>,
) -> Result<(), AppError> {
    Ok(())
}
