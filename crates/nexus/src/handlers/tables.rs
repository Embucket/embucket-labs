use std::ops::Deref;
use std::result::Result;
use axum::{Json, extract::State, extract::Path};
use axum_macros::debug_handler;
use uuid::Uuid;

use catalog::service::{
    CatalogService,
    Catalog,
};
use catalog::repository::Repository;

use crate::state::AppState;
use crate::error::AppError;
use crate::schemas::tables::{
    CreateTableSchema,
    TableSchema,
};
use catalog::models::TableCreation;
use catalog::models::NamespaceIdent;


#[debug_handler]
pub async fn create_table(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
    Json(payload): Json<CreateTableSchema>,
) -> Result<Json<TableSchema>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogService::new(
        state.catalog_repo.clone(),
        wh,
    );
    let namespace_id = NamespaceIdent::new(namespace_id);
    let table = catalog.create_table(
        &namespace_id,
        payload.into(),
    ).await?;

    
    Ok(Json(TableSchema {
        metadata_location: None,
        metadata: table.metadata().clone(),
        config: None,
    }))
}
