use super::with_derives;
#[cfg(feature = "serde")] use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")] use utoipa::ToSchema;

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
