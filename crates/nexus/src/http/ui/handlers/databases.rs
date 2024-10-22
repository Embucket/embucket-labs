use crate::error::AppError;
use crate::http::ui::models::aws;
use crate::http::ui::models::database;
use crate::http::ui::models::storage_profile;
use crate::http::ui::models::table::Statistics;
use crate::http::ui::models::warehouse;
use crate::state::AppState;
use axum::{extract::Path, extract::State, Json};
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
            database::CreateDatabasePayload,
            database::Database
        )
    ),
    tags(
        (name = "Databases", description = "Databases management endpoints.")
    )
)]
struct ApiDoc;

#[utoipa::path(
    post,
    path = "/warehouses/{warehouseId}/databases/{databaseName}",
    operation_id = "createDatabase",
    responses(
        (status = 200, description = "Successful Response", body = database::Database),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn create_database(
    State(state): State<AppState>,
    Path(payload): Path<database::CreateDatabasePayload>,
) -> Result<Json<database::Database>, AppError> {
    Ok(Json(database::Database {
        name: "".to_string(),
        properties: None,
        id: Default::default(),
        warehouse_id: Default::default(),
    }))
}

#[utoipa::path(
    delete,
    path = "/warehouses/{warehouseId}/databases/{databaseName}",
    operation_id = "deleteDatabase",
    responses(
        (status = 204, description = "Successful Response"),
        (status = 404, description = "Database not found")
    )
)]
pub async fn delete_database(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
) -> Result<(), AppError> {
    Ok(())
}

#[utoipa::path(
    get,
    path = "/warehouses/{warehouseId}/databases/{databaseName}",
    operation_id = "databaseDashboard",
    responses(
        (status = 200, description = "Successful Response", body = database::DatabaseDashboard),
        (status = 204, description = "Successful Response"),
        (status = 404, description = "Database not found")
    )
)]
pub async fn get_database(
    State(state): State<AppState>,
    Path((warehouse_id, database_name)): Path<(Uuid, String)>,
) -> Result<Json<database::DatabaseDashboard>, AppError> {
    Ok(Json(database::DatabaseDashboard {
        name: "".to_string(),
        properties: None,
        id: Default::default(),
        warehouse_id: Default::default(),
        warehouse: warehouse::WarehouseEntity {
            name: "".to_string(),
            storage_profile_id: Default::default(),
            key_prefix: "".to_string(),
            id: Default::default(),
            external_id: Default::default(),
            location: "".to_string(),
            created_at: Default::default(),
            updated_at: Default::default(),
            storage_profile: storage_profile::StorageProfile {
                r#type: aws::CloudProvider::S3,
                region: "".to_string(),
                bucket: "".to_string(),
                credentials: Default::default(),
                sts_role_arn: None,
                endpoint: None,
                id: Default::default(),
                created_at: Default::default(),
                updated_at: Default::default(),
            },
        },
        tables: vec![],
        statistics: Statistics {
            commit_count: 0,
            op_append_count: 0,
            op_overwrite_count: 0,
            op_delete_count: 0,
            op_replace_count: 0,
            total_bytes: 0,
            bytes_added: 0,
            bytes_removed: 0,
            total_rows: 0,
            rows_added: 0,
            rows_deleted: 0,
            table_count: None,
            database_count: None,
        },
        compaction_summary: None,
    }))
}
