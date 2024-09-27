pub mod storage_profiles;
pub mod warehouses;
pub mod namespaces;
pub mod tables;

use std::result::Result;
use axum::{Json, extract::State, extract::Path};
use axum_macros::debug_handler;
use uuid::Uuid;
use crate::schemas::Config;
use catalog::service::{
    CatalogService,
    Catalog,
};

use crate::state::AppState;
use crate::error::AppError;


#[debug_handler]
pub async fn get_config(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Config>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogService::new(
        state.catalog_repo.clone(),
        wh,
    );
    let config = catalog.get_config().await?;
    
    Ok(Json(config.into()))
}