#![allow(clippy::needless_for_each)]
use crate::dashboard::error::{HistorySnafu, MetastoreSnafu};
use crate::dashboard::models::{Dashboard, DashboardResponse};
use crate::error::{ErrorResponse, Result};
use crate::state::AppState;
use axum::{Json, extract::State};
use core_history::GetQueriesParams;
use core_metastore::error::UtilSlateDBSnafu;
use core_metastore::ListParams;
use core_utils::scan_iterator::ScanIterator;
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
    let rw_databases = state
        .metastore
        .get_databases(ListParams::default())
        .await
        .context(MetastoreSnafu)?;
    let total_databases = rw_databases.len();
    let mut total_schemas = 0;
    let mut total_tables = 0;
    for rw_database in rw_databases {
        let rw_schemas = state
            .metastore
            .iter_schemas(&rw_database.ident.clone())
            .collect()
            .await
            .context(UtilSlateDBSnafu)
            .context(MetastoreSnafu)?;
        total_schemas += rw_schemas.len();
        for rw_schema in rw_schemas {
            total_tables += state
                .metastore
                .iter_tables(&rw_schema.ident)
                .collect()
                .await
                .context(UtilSlateDBSnafu)
                .context(MetastoreSnafu)?
                .len();
        }
    }

    let total_queries = state
        .history_store
        .get_queries(GetQueriesParams::new())
        .await
        .context(HistorySnafu)?
        .len();

    Ok(Json(DashboardResponse(Dashboard {
        total_databases,
        total_schemas,
        total_tables,
        total_queries,
    })))
}
