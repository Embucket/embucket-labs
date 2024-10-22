use crate::http::ui::models::aws::{CloudProvider, Credentials};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct CreateStorageProfilePayload {
    #[serde(rename = "type")]
    pub r#type: CloudProvider,
    #[validate(length(min = 1))]
    pub region: String,
    #[validate(length(min = 6, max = 63))]
    pub bucket: String,
    pub credentials: Credentials,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sts_role_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

impl CreateStorageProfilePayload {
    #[allow(clippy::new_without_default)]
    pub fn new(
        r#type: CloudProvider,
        region: String,
        bucket: String,
        credentials: Credentials,
    ) -> CreateStorageProfilePayload {
        CreateStorageProfilePayload {
            r#type,
            region,
            bucket,
            credentials,
            sts_role_arn: None,
            endpoint: None,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct StorageProfile {
    #[serde(rename = "type")]
    pub r#type: CloudProvider,
    #[validate(length(min = 1))]
    pub region: String,
    #[validate(length(min = 6, max = 63))]
    pub bucket: String,
    pub credentials: Credentials,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sts_role_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub id: uuid::Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl StorageProfile {
    #[allow(clippy::new_without_default)]
    pub fn new(
        r#type: CloudProvider,
        region: String,
        bucket: String,
        credentials: Credentials,
        id: uuid::Uuid,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> StorageProfile {
        StorageProfile {
            r#type,
            region,
            bucket,
            credentials,
            sts_role_arn: None,
            endpoint: None,
            id,
            created_at,
            updated_at,
        }
    }
}
