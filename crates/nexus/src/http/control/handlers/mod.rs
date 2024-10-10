pub mod namespaces;
pub mod storage_profiles;
pub mod tables;
pub mod warehouses;

use crate::http::control::schemas::Config;
use axum::{extract::Path, extract::State, Json};
use axum_macros::debug_handler;
use catalog::service::{Catalog, CatalogImpl};
use std::result::Result;
use uuid::Uuid;

use crate::error::AppError;
use crate::state::AppState;

#[debug_handler]
pub async fn get_config(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Config>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogImpl::new(state.catalog_repo.clone(), wh);
    let config = catalog.get_config().await?;

    Ok(Json(config.into()))
}
