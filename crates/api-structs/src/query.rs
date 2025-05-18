use super::with_derives;
#[cfg(feature = "schema")] use utoipa::ToSchema;
#[cfg(feature = "serde")]  use serde::{Deserialize, Serialize};
use std::collections::HashMap;

with_derives! {
    #[derive(Clone, Debug)]
    pub struct QueryCreatePayload {
        pub worksheet_id: Option<i64>,
        pub query: String,
        pub context: Option<HashMap<String, String>>,
    }
}
