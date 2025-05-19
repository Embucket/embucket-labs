use super::with_derives;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use utoipa::ToSchema;

with_derives! {
    #[derive(Debug, Clone)]
    pub struct TableUploadPayload {
        #[schema(format = "binary")]
        pub upload_file: String,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct TableUploadResponse {
        pub count: usize,
        pub duration_ms: u128,
    }
}
