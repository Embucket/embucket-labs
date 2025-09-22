use super::handlers::{abort, get_query, login, query};
use super::state::AppState;
use axum::Router;
use axum::routing::{get, post};

use super::layer::require_auth;
use super::server_models::Config;
use super::state;
use axum::middleware;
use core_executor::service::CoreExecutionService;
use core_executor::utils::Config as UtilsConfig;
use core_history::store::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::decompression::RequestDecompressionLayer;

pub fn create_auth_router() -> Router<AppState> {
    Router::new().route("/session/v1/login-request", post(login))
}

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/queries/v1/query-request", post(query))
        .route("/queries/v1/abort-request", post(abort))
        .route("/queries/{queryId}/result", get(get_query))
}

// TODO: We should consider using this by both main and tests
#[allow(clippy::needless_pass_by_value, clippy::expect_used)]
pub async fn make_app(
    metastore: Arc<SlateDBMetastore>,
    history_store: Arc<SlateDBHistoryStore>,
    snowflake_rest_cfg: Config,
    execution_cfg: UtilsConfig,
) -> Result<Router, Box<dyn std::error::Error>> {
    let execution_svc = Arc::new(
        CoreExecutionService::new(metastore, history_store, Arc::new(execution_cfg))
            .await
            .expect("Failed to create execution service"),
    );

    // Create the application state

    let snowflake_state = state::AppState {
        execution_svc,
        config: snowflake_rest_cfg,
    };

    let compression_layer = ServiceBuilder::new()
        .layer(CompressionLayer::new())
        .layer(RequestDecompressionLayer::new());

    let snowflake_router = create_router()
        .with_state(snowflake_state.clone())
        .layer(compression_layer.clone())
        .layer(middleware::from_fn_with_state(
            snowflake_state.clone(),
            require_auth,
        ));
    let snowflake_auth_router = create_auth_router()
        .with_state(snowflake_state)
        .layer(compression_layer);
    let snowflake_router = snowflake_router.merge(snowflake_auth_router);

    let router = Router::new().merge(snowflake_router);

    Ok(router)
}
