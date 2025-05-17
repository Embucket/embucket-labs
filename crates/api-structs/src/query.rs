#[cfg(feature = "schema")] use utoipa::ToSchema;
#[cfg(feature = "serde")]  use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone)]
#[cfg_attr(feature = "schema", derive(ToSchema))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "partial", derive(PartialEq))]
#[cfg_attr(feature = "eq", derive(Eq))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub struct QueryCreatePayload {
    pub worksheet_id: Option<i64>,
    pub query: String,
    pub context: Option<HashMap<String, String>>,
}
