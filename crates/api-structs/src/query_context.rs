use super::with_derives;
#[cfg(feature = "schema")] use utoipa::ToSchema;
#[cfg(feature = "serde")]  use serde::{Deserialize, Serialize};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::array::RecordBatch;
use std::collections::HashMap;

with_derives! {
#[derive(Default, Debug, Clone)]
    pub struct QueryContext {
        pub database: Option<String>,
        pub schema: Option<String>,
        // TODO: Remove this
        pub worksheet_id: Option<i64>,
    }
}

impl QueryContext {
    #[must_use]
    pub const fn new(
        database: Option<String>,
        schema: Option<String>,
        worksheet_id: Option<i64>,
    ) -> Self {
        Self {
            database,
            schema,
            worksheet_id,
        }
    }
}
