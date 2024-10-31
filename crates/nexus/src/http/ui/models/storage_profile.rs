use crate::http::ui::models::aws::{CloudProvider, Credentials};
use chrono::{DateTime, Utc};
use control_plane::models;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
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

impl From<CreateStorageProfilePayload> for models::StorageProfileCreateRequest {
    fn from(payload: CreateStorageProfilePayload) -> Self {
        models::StorageProfileCreateRequest {
            r#type: payload.r#type.into(),
            region: payload.region,
            bucket: payload.bucket,
            credentials: payload.credentials.into(),
            sts_role_arn: payload.sts_role_arn,
            endpoint: payload.endpoint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl StorageProfile {
    #[allow(clippy::new_without_default)]
    pub fn new(
        r#type: CloudProvider,
        region: String,
        bucket: String,
        credentials: Credentials,
        id: uuid::Uuid,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
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

impl From<models::StorageProfile> for StorageProfile {
    fn from(profile: models::StorageProfile) -> Self {
        StorageProfile {
            r#type: profile.r#type.into(),
            region: profile.region,
            bucket: profile.bucket,
            credentials: profile.credentials.into(),
            sts_role_arn: profile.sts_role_arn,
            endpoint: profile.endpoint,
            id: profile.id,
            created_at: DateTime::from_naive_utc_and_offset(profile.created_at, Utc),
            updated_at: DateTime::from_naive_utc_and_offset(profile.updated_at, Utc),
        }
    }
}
