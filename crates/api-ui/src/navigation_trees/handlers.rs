use crate::error::ErrorResponse;
use crate::navigation_trees::error::{NavigationTreesAPIError, NavigationTreesResult};
use crate::navigation_trees::models::{
    NavigationTreeDatabase, NavigationTreeSchema, NavigationTreeTable, NavigationTreesParameters,
    NavigationTreesResponse,
};
use crate::state::AppState;
use api_sessions::DFSessionId;
use axum::extract::Query;
use axum::{Json, extract::State};
use core_executor::error::ExecutionError;
use core_executor::query::QueryContext;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::common::DataFusionError;
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
    let (database_batches, _) = state
        .execution_svc
        .query(
            &session_id,
            "SELECT * FROM information_schema.databases",
            QueryContext::default(),
        )
        .await
        .map_err(|e| NavigationTreesAPIError::Execution { source: e })?;

    let mut catalogs_tree: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();

    for db_batch in database_batches {
        let catalog_col = downcast_string_column(&db_batch, "database_name")?;

        for i in 0..db_batch.num_rows() {
            let catalog = catalog_col.value(i);

            let query = format!(
                "SELECT table_schema, table_name FROM {catalog}.information_schema.tables",
            );

            let (table_batches, _) = state
                .execution_svc
                .query(&session_id, &query, QueryContext::default())
                .await
                .map_err(|e| NavigationTreesAPIError::Execution { source: e })?;

            for table_batch in table_batches {
                let schema_col = downcast_string_column(&table_batch, "table_schema")?;
                let name_col = downcast_string_column(&table_batch, "table_name")?;

                for j in 0..table_batch.num_rows() {
                    let schema = schema_col.value(j).to_string();
                    let name = name_col.value(j).to_string();

                    catalogs_tree
                        .entry(catalog.to_string())
                        .or_default()
                        .entry(schema)
                        .or_default()
                        .push(name);
                }
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

fn downcast_string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, NavigationTreesAPIError> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| NavigationTreesAPIError::Execution {
            source: ExecutionError::DataFusion {
                source: DataFusionError::Internal(format!("Missing or invalid column: '{name}'")),
            },
        })
}
