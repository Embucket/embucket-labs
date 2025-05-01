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
        ("cursor" = Option<String>, Query, description = "Navigation trees cursor"),
        ("limit" = Option<usize>, Query, description = "Navigation trees limit"),
    ),
    tags = ["navigation-trees"],
    path = "/ui/navigation-trees",
    responses(
        (status = 200, description = "Successful Response", body = NavigationTreesResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn get_navigation_trees(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<NavigationTreesParameters>,
    State(state): State<AppState>,
) -> NavigationTreesResult<Json<NavigationTreesResponse>> {
    let session = state
        .execution_svc
        .create_session(session_id)
        .await
        .context(error::SessionSnafu)?;
    let catalogs_tree = session.fetch_catalogs_tree();

    let mut items = Vec::new();
    for (catalog_name, schemas_map) in catalogs_tree {
        let mut schemas = Vec::new();

        for (schema_name, table_names) in schemas_map {
            let tables = table_names
                .into_iter()
                .map(|table_name| NavigationTreeTable { name: table_name })
                .collect();
            schemas.push(NavigationTreeSchema {
                name: schema_name,
                tables,
            });
        }
        items.push(NavigationTreeDatabase {
            name: catalog_name,
            schemas,
        });
    }

    Ok(Json(NavigationTreesResponse {
        items,
        current_cursor: parameters.cursor,
        next_cursor: String::new(),
    }))
}
