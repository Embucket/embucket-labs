use crate::http::error::ErrorResponse;
use crate::http::state::AppState;
use crate::http::ui::navigation_trees::error::{NavigationTreesAPIError, NavigationTreesResult};
use crate::http::ui::navigation_trees::models::{
    NavigationTreeDatabase, NavigationTreeSchema, NavigationTreeTable, NavigationTreesParameters,
    NavigationTreesResponse,
};
use axum::extract::Query;
use axum::{extract::State, Json};
use embucket_metastore::error::MetastoreError;
use embucket_utils::scan_iterator::ScanIterator;
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
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_navigation_trees(
    Query(parameters): Query<NavigationTreesParameters>,
    State(state): State<AppState>,
) -> NavigationTreesResult<Json<NavigationTreesResponse>> {
    let rw_databases = state
        .metastore
        .iter_databases()
        .cursor(parameters.cursor.clone())
        .limit(parameters.limit)
        .collect()
        .await
        .map_err(|e| NavigationTreesAPIError::Get {
            source: MetastoreError::UtilSlateDB { source: e },
        })?;

    let next_cursor = rw_databases
        .iter()
        .last()
        .map_or(String::new(), |rw_object| rw_object.ident.clone());

    let mut databases: Vec<NavigationTreeDatabase> = vec![];
    for rw_database in rw_databases {
        let rw_schemas = state
            .metastore
            .iter_schemas(&rw_database.ident.clone())
            .collect()
            .await
            .map_err(|e| NavigationTreesAPIError::Get {
                source: MetastoreError::UtilSlateDB { source: e },
            })?;

        let mut schemas: Vec<NavigationTreeSchema> = vec![];
        for rw_schema in rw_schemas {
            let rw_tables = state
                .metastore
                .iter_tables(&rw_schema.ident)
                .collect()
                .await
                .map_err(|e| NavigationTreesAPIError::Get {
                    source: MetastoreError::UtilSlateDB { source: e },
                })?;

            let mut tables: Vec<NavigationTreeTable> = vec![];
            for rw_table in rw_tables {
                tables.push(NavigationTreeTable {
                    name: rw_table.ident.table.clone(),
                });
            }
            schemas.push(NavigationTreeSchema {
                name: rw_schema.ident.schema.clone(),
                tables,
            });
        }
        databases.push(NavigationTreeDatabase {
            name: rw_database.ident.clone(),
            schemas,
        });
    }

    Ok(Json(NavigationTreesResponse {
        items: databases,
        current_cursor: parameters.cursor,
        next_cursor,
    }))
}
