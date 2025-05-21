use core_executor::error::ExecutionError;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::common::DataFusionError;
use serde::Deserialize;
use std::fmt::Display;
use utoipa::{IntoParams, ToSchema};

pub mod auth;
pub mod config;
pub mod dashboard;
pub mod databases;
pub mod error;
pub mod layers;
pub mod navigation_trees;
pub mod queries;
pub mod router;
pub mod schemas;
pub mod state;
pub mod tables;
#[cfg(test)]
pub mod tests;
pub mod volumes;
pub mod web_assets;
pub mod worksheets;

//Default limit for pagination
#[allow(clippy::unnecessary_wraps)]
const fn default_limit() -> Option<u16> {
    Some(250)
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct SearchParameters {
    pub offset: Option<usize>,
    #[serde(default = "default_limit")]
    pub limit: Option<u16>,
    pub search: Option<String>,
    pub order_by: Option<String>,
    pub order_direction: Option<OrderDirection>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderDirection {
    ASC,
    DESC,
}

impl Default for OrderDirection {
    fn default() -> Self {
        Self::DESC
    }
}

impl Display for OrderDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ASC => write!(f, "ASC"),
            Self::DESC => write!(f, "DESC"),
        }
    }
}

fn downcast_string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, ExecutionError> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ExecutionError::DataFusion {
            source: DataFusionError::Internal(format!("Missing or invalid column: '{name}'")),
        })
}

fn downcast_int64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, ExecutionError> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| ExecutionError::DataFusion {
            source: DataFusionError::Internal(format!("Missing or invalid column: '{name}'")),
        })
}

fn apply_parameters(
    sql_string: &str,
    parameters: SearchParameters,
    search_columns: &[&str],
) -> String {
    let sql_string = parameters.search.map_or_else(
        || sql_string.to_string(),
        |search| {
            let separator = format!(" ILIKE '%{search}%' OR ");
            let sql_string = sql_string.find("WHERE").map_or_else(
                || {
                    //if there is no WHERE clause
                    let sql_string =
                        format!("{sql_string} WHERE ({}", search_columns.join(&separator));
                    sql_string
                },
                |_| {
                    //if there is a WHERE clause
                    let sql_string =
                        format!("{sql_string} AND ({}", search_columns.join(&separator));
                    sql_string
                },
            );
            format!("{sql_string} ILIKE '%{search}%')")
        },
    );
    //Default order by is the first search column or created at
    let sql_string = parameters.order_by.map_or_else(
        || {
            format!(
                "{sql_string} ORDER BY {}",
                search_columns.first().unwrap_or(&"created_at")
            )
        },
        |order_by| format!("{sql_string} ORDER BY {order_by}"),
    );
    let sql_string = parameters.order_direction.map_or_else(
        || format!("{sql_string} DESC"),
        |order_direction| format!("{sql_string} {order_direction}"),
    );
    let sql_string = parameters.offset.map_or_else(
        || sql_string.clone(),
        |offset| format!("{sql_string} OFFSET {offset}"),
    );

    parameters.limit.map_or_else(
        || sql_string.clone(),
        |limit| format!("{sql_string} LIMIT {limit}"),
    )
}
