use crate::http::ui::models::aws::{CloudProvider, Credentials};
use chrono::{DateTime, Utc};
use control_plane::models;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateStorageProfilePayload {
    #[serde(rename = "type")]
    pub r#type: CloudProvider,
    #[validate(length(min = 1))]
    pub region: Option<String>,
    #[validate(length(min = 6, max = 63))]
    pub bucket: Option<String>,
    pub credentials: Option<Credentials>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sts_role_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

impl From<CreateStorageProfilePayload> for models::StorageProfileCreateRequest {
    fn from(payload: CreateStorageProfilePayload) -> Self {
        Self {
            r#type: payload.r#type.into(),
            region: payload.region,
            bucket: payload.bucket,
            credentials: payload.credentials.map(std::convert::Into::into),
            sts_role_arn: payload.sts_role_arn,
            endpoint: payload.endpoint,
            validate_credentials: Option::from(false),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageProfile {
    #[serde(rename = "type")]
    pub r#type: CloudProvider,
    #[validate(length(min = 1))]
    pub region: Option<String>,
    #[validate(length(min = 6, max = 63))]
    pub bucket: Option<String>,
    pub credentials: Option<Credentials>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sts_role_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<models::StorageProfile> for StorageProfile {
    fn from(profile: models::StorageProfile) -> Self {
        Self {
            r#type: profile.r#type.into(),
            region: profile.region,
            bucket: profile.bucket,
            credentials: profile.credentials.map(std::convert::Into::into),
            sts_role_arn: profile.sts_role_arn,
            endpoint: profile.endpoint,
            id: profile.id,
            created_at: DateTime::from_naive_utc_and_offset(profile.created_at, Utc),
            updated_at: DateTime::from_naive_utc_and_offset(profile.updated_at, Utc),
        }
    }
}
