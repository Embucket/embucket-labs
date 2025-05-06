use crate::http::error::ErrorResponse;
use crate::http::session::DFSessionId;
use crate::http::state::AppState;
use crate::http::ui::navigation_trees::error::{self as error, NavigationTreesResult};
use crate::http::ui::navigation_trees::models::{
    NavigationTreeDatabase, NavigationTreeSchema, NavigationTreeTable, NavigationTreesParameters,
    NavigationTreesResponse,
};
use axum::extract::Query;
use axum::{extract::State, Json};
use snafu::ResultExt;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_navigation_trees,
    ),
    components(
        schemas(
            NavigationTreesResponse,
            NavigationTreeDatabase,
            NavigationTreeSchema,
            NavigationTreeTable,
            ErrorResponse,
        )
    ),
    tags(
        (name = "navigation-trees", description = "Navigation trees endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    operation_id = "getNavigationTrees",
    params(
        ("offset" = Option<usize>, Query, description = "Navigation trees offset"),
        ("limit" = Option<u16>, Query, description = "Navigation trees limit"),
    ),
    tags = ["navigation-trees"],
    path = "/ui/navigation-trees",
    responses(
        (status = 200, description = "Successful Response", body = NavigationTreesResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_navigation_trees(
    DFSessionId(session_id): DFSessionId,
    Query(params): Query<NavigationTreesParameters>,
    State(state): State<AppState>,
) -> NavigationTreesResult<Json<NavigationTreesResponse>> {
    let catalogs_tree = state
        .execution_svc
        .create_session(session_id)
        .await
        .context(error::SessionSnafu)?
        .fetch_catalogs_tree();

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.map_or(usize::MAX, usize::from);

    let items = catalogs_tree
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(catalog_name, schemas_map)| NavigationTreeDatabase {
            name: catalog_name,
            schemas: schemas_map
                .into_iter()
                .map(|(schema_name, table_names)| NavigationTreeSchema {
                    name: schema_name,
                    tables: table_names
                        .into_iter()
                        .map(|name| NavigationTreeTable { name })
                        .collect(),
                })
                .collect(),
        })
        .collect();

    Ok(Json(NavigationTreesResponse { items }))
}
