use super::handlers::login;
use super::state::AppState;
use axum::Router;
use axum::routing::post;

pub fn create_router() -> Router<AppState> {
    // TODO: move this to the layer.rs
    Router::new().route("/session/v1/login-request", post(login))
}
