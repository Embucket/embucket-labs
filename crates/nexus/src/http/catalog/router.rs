use crate::state::AppState;
use axum::routing::{delete, get, post};
use axum::Router;

fn create_router() -> Router<AppState> {
    let table_router: Router<AppState> = Router::new()
        .route("/", post(create_table))
        .route("/:table", get(get_table))
        .route("/:table", delete(delete_table))
        .route("/:table", post(update_table));

    let ns_router = Router::new()
        .route("/", get(list_namespaces))
        .router("/", post(create_namespace))
        .route("/:namespace", get(get_namespace))
        .route("/:namespace", delete(delete_namespace))
        .nest("/:namespace/tables", table_router);

    Router::new()
        .route("/:wid/v1/config", get(get_config))
        .nest("/:wid/v1/namespaces", ns_router)
}
