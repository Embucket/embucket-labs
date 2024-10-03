use axum::{extract::Path, extract::State, Json};
use axum_macros::debug_handler;
use std::ops::Deref;
use std::result::Result;
use utoipa::OpenApi;
use uuid::Uuid;

use catalog::repository::Repository;
use catalog::service::{Catalog, CatalogService};

use crate::error::AppError;
use crate::schemas::tables::{CreateTableSchema, TableSchema};
use crate::state::AppState;
use catalog::models::NamespaceIdent;
use catalog::models::TableIdent;

#[derive(OpenApi)]
#[openapi(
    paths(create_table,),
    components(schemas(CreateTableSchema, TableSchema),)
)]
pub struct TableApi;

#[utoipa::path(
    post, 
    operation_id = "createTable",
    path = "", 
    params(("warehouseId" = Uuid, description = "Warehouse ID"), ("namespaceId" = String, description = "Namespace ID")),
    request_body = CreateTableSchema,
    responses((status = 200, body = TableSchema))
)]
pub async fn create_table(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
    Json(payload): Json<CreateTableSchema>,
) -> Result<Json<TableSchema>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogService::new(state.catalog_repo.clone(), wh);
    let namespace_id = NamespaceIdent::new(namespace_id);
    let table = catalog.create_table(&namespace_id, payload.into()).await?;

    Ok(Json(TableSchema {
        metadata_location: None,
        metadata: table.metadata().clone(),
        config: None,
    }))
}


#[utoipa::path(
    get, 
    operation_id = "getTable",
    path = "", 
    params(("warehouseId" = Uuid, description = "Warehouse ID"), ("namespaceId" = String, description = "Namespace ID"), ("tableId" = String, description = "Table ID")),
    responses((status = 200, body = TableSchema)),
)]
pub async fn get_table(
    State(state): State<AppState>,
    Path((id, namespace_id, table_id)): Path<(Uuid, String, String)>,
) -> Result<Json<TableSchema>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogService::new(state.catalog_repo.clone(), wh);
    let namespace_id = NamespaceIdent::new(namespace_id);
    let table_id = TableIdent::new(namespace_id, table_id);
    let table = catalog.load_table(&table_id).await?;

    Ok(Json(TableSchema {
        metadata_location: None,
        metadata: table.metadata().clone(),
        config: None,
    }))
}
