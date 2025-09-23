use core_executor::error::{self as ex_error};
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
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
pub mod test_server;

#[cfg(test)]
pub mod tests;
pub mod volumes;
pub mod web_assets;
pub mod worksheets;

pub use error::{Error, Result};

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

impl Display for SearchParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = "";
        let str = self
            .offset
            .map_or_else(|| str.to_string(), |offset| format!("{str}offset={offset}"));
        let str = self.limit.map_or_else(
            || str.to_string(),
            |limit| {
                format!(
                    "{str}{}limit={limit}",
                    if str.is_empty() { "" } else { "&" }
                )
            },
        );
        let str = self.search.clone().map_or_else(
            || str.to_string(),
            |search| {
                format!(
                    "{str}{}search={search}",
                    if str.is_empty() { "" } else { "&" }
                )
            },
        );
        let str = self.order_by.clone().map_or_else(
            || str.to_string(),
            |order_by| {
                format!(
                    "{str}{}orderBy={order_by}",
                    if str.is_empty() { "" } else { "&" }
                )
            },
        );
        let str = self.order_direction.clone().map_or_else(
            || str.to_string(),
            |order_direction| {
                format!(
                    "{str}{}orderDirection={order_direction}",
                    if str.is_empty() { "" } else { "&" }
                )
            },
        );
        write!(f, "{str}")
    }
}

#[derive(Debug, Deserialize, ToSchema, Copy, Clone)]
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
) -> std::result::Result<&'a StringArray, core_executor::Error> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ex_error::MissingOrInvalidColumnSnafu { name }.build())
}

fn downcast_int64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> std::result::Result<&'a Int64Array, core_executor::Error> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| ex_error::MissingOrInvalidColumnSnafu { name }.build())
}

fn apply_parameters(
    sql_string: &str,
    parameters: SearchParameters,
    search_columns: &[&str],
    default_order_by_column: &str,
    default_order_direction: OrderDirection,
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
        || format!("{sql_string} ORDER BY {default_order_by_column}"),
        |order_by| format!("{sql_string} ORDER BY {order_by}"),
    );
    let sql_string = parameters.order_direction.map_or_else(
        || format!("{sql_string} {default_order_direction}"),
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
