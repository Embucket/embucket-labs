use super::error::ExecutionSnafu;
use crate::downcast_string_column;
use crate::error::ErrorResponse;
use crate::navigation_trees::error::NavigationTreesResult;
use crate::navigation_trees::models::{
    NavigationTreeDatabase, NavigationTreeSchema, NavigationTreeTable, NavigationTreesParameters,
    NavigationTreesResponse,
};
use crate::state::AppState;
use api_sessions::DFSessionId;
use axum::extract::Query;
use axum::{Json, extract::State};
use core_executor::models::{QueryContext, QueryResult};
use snafu::ResultExt;
use std::collections::BTreeMap;
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
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn get_navigation_trees(
    DFSessionId(session_id): DFSessionId,
    Query(params): Query<NavigationTreesParameters>,
    State(state): State<AppState>,
) -> NavigationTreesResult<Json<NavigationTreesResponse>> {
    let QueryResult {
        records: tree_batches,
        ..
    } = state
        .execution_svc
        .query(
            &session_id,
            "SELECT * FROM information_schema.navigation_tree",
            QueryContext::default(),
        )
        .await
        .context(ExecutionSnafu)?;

    let mut catalogs_tree: BTreeMap<String, BTreeMap<String, Vec<(String, String)>>> =
        BTreeMap::new();

    for batch in tree_batches {
        let databases = downcast_string_column(&batch, "database").context(ExecutionSnafu)?;
        let schemas = downcast_string_column(&batch, "schema").context(ExecutionSnafu)?;
        let tables = downcast_string_column(&batch, "table").context(ExecutionSnafu)?;
        let table_types = downcast_string_column(&batch, "table_type").context(ExecutionSnafu)?;
        for j in 0..batch.num_rows() {
            let database = databases.value(j).to_string();
            let schema = schemas.value(j).to_string();
            let table = tables.value(j).to_string();
            let table_type = table_types.value(j).to_string();

            let db_entry = catalogs_tree.entry(database).or_default();

            if schema.is_empty() {
                continue;
            }
            let schema_entry = db_entry.entry(schema).or_default();
            if !table.is_empty() {
                schema_entry.push((table, table_type));
            }
        }
    }

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
                .map(|(schema_name, table_names)| {
                    let tables = table_names
                        .iter()
                        .filter(|(_, table_type)| table_type != "VIEW")
                        .map(|(name, _)| NavigationTreeTable { name: name.clone() })
                        .collect();
                    let views = table_names
                        .iter()
                        .filter(|(_, table_type)| table_type == "VIEW")
                        .map(|(name, _)| NavigationTreeTable { name: name.clone() })
                        .collect();
                    NavigationTreeSchema {
                        name: schema_name,
                        tables,
                        views,
                    }
                })
                .collect(),
        })
        .collect();

    Ok(Json(NavigationTreesResponse { items }))
}
