use crate::state::State;
use axum::Router;
use axum::routing::{delete, get, post, put};

use crate::handlers::{create_volume, delete_volume, get_volume, list_volumes, update_volume};

pub fn create_router() -> Router<State> {
    Router::new()
        .route("/volumes", get(list_volumes))
        .route("/volumes", post(create_volume))
        .route("/volumes/{volumeName}", get(get_volume))
        .route("/volumes/{volumeName}", put(update_volume))
        .route("/volumes/{volumeName}", delete(delete_volume))
}
