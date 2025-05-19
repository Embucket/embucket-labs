use core_executor::error::ExecutionError;
use datafusion::arrow::array::{RecordBatch, StringArray};
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
