#![allow(clippy::needless_for_each)]
use crate::dashboard::error::{HistorySnafu, MetastoreSnafu};
use crate::dashboard::models::{Dashboard, DashboardResponse};
use crate::error::{ErrorResponse, Result};
use crate::state::AppState;
use axum::{Json, extract::State};
use core_history::GetQueriesParams;
use snafu::ResultExt;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_dashboard,
    ),
    components(
        schemas(
            DashboardResponse,
        )
    ),
    tags(
        (name = "dashboard", description = "Dashboard endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    operation_id = "getDashboard",
    tags = ["dashboard"],
    path = "/ui/dashboard",
    responses(
        (status = 200, description = "Successful Response", body = DashboardResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(name = "api_ui::get_dashboard", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_dashboard(State(state): State<AppState>) -> Result<Json<DashboardResponse>> {
    let stats = state.metastore.get_stats().await.context(MetastoreSnafu)?;

    let total_queries = state
        .history_store
        .get_queries(GetQueriesParams::new())
        .await
        .context(HistorySnafu)?
        .len();

    Ok(Json(DashboardResponse(Dashboard {
        total_databases: stats.total_databases,
        total_schemas: stats.total_schemas,
        total_tables: stats.total_tables,
        total_queries,
    })))
}
