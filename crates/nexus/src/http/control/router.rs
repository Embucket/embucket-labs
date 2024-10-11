use crate::state::AppState;
use axum::routing::{delete, get, post};
use axum::Router;

use crate::http::control::handlers::storage_profiles::{
    create_storage_profile, delete_storage_profile, get_storage_profile, list_storage_profiles,
};
use crate::http::control::handlers::warehouses::{
    create_warehouse, delete_warehouse, get_warehouse, list_warehouses,
};

pub fn create_router() -> Router<AppState> {
    let sp_router = Router::new()
        .route("/", post(create_storage_profile))
        .route("/:id", get(get_storage_profile))
        .route("/:id", delete(delete_storage_profile))
        .route("/", get(list_storage_profiles));

    let wh_router = Router::new()
        .route("/", post(create_warehouse))
        .route("/:id", get(get_warehouse))
        .route("/:id", delete(delete_warehouse))
        .route("/", get(list_warehouses));

    Router::new()
        .nest("/v1/storage-profile", sp_router)
        .nest("/v1/warehouse", wh_router)
}
