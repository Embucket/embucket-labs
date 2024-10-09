use crate::error::Error;
use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use uuid::Uuid;

// Enum for supported cloud providers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloudProvider {
    AWS,
    AZURE,
    GCS,
}

// AWS Access Key Credentials
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AwsAccessKeyCredential {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
}

// AWS Role Credentials
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AwsRoleCredential {
    pub role_arn: String,
    pub external_id: String,
}

// Composite enum for credentials
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "credential_type")] // Enables tagged union based on credential type
#[serde(rename_all = "kebab-case")]
pub enum Credentials {
    #[serde(rename = "access_key")]
    AccessKey(AwsAccessKeyCredential),

    #[serde(rename = "role")]
    Role(AwsRoleCredential),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct StorageProfile {
    pub id: Uuid,
    pub r#type: CloudProvider,
    pub region: String,
    pub bucket: String,
    pub credentials: Credentials,
    pub sts_role_arn: Option<String>,
    pub endpoint: Option<String>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct StorageProfileCreateRequest {
    #[serde(rename = "type")]
    pub r#type: CloudProvider,
    pub region: String,
    pub bucket: String,
    pub credentials: Credentials,
    pub sts_role_arn: Option<String>,
    pub endpoint: Option<String>,
}

impl TryFrom<&StorageProfileCreateRequest> for StorageProfile {
    type Error = Error;

    fn try_from(value: &StorageProfileCreateRequest) -> Result<Self, Self::Error> {
        StorageProfile::new(
            value.r#type,
            value.region.clone(),
            value.bucket.clone(),
            value.credentials.clone(),
            value.sts_role_arn.clone(),
            value.endpoint.clone(),
        )
    }
}

impl TryFrom<StorageProfileCreateRequest> for StorageProfile {
    type Error = Error;

    fn try_from(value: StorageProfileCreateRequest) -> Result<Self, Self::Error> {
        StorageProfile::new(
            value.r#type,
            value.region,
            value.bucket,
            value.credentials,
            value.sts_role_arn,
            value.endpoint,
        )
    }
}

impl StorageProfile {
    // Method to create a new StorageProfile with validation
    pub fn new(
        cloud_provider: CloudProvider,
        region: String,
        bucket: String,
        credentials: Credentials,
        sts_role_arn: Option<String>,
        endpoint: Option<String>,
    ) -> Result<Self, Error> {
        // Example validation: Ensure bucket name length
        if bucket.len() < 6 || bucket.len() > 63 {
            return Err(Error::InvalidInput(
                "Bucket name must be between 6 and 63 characters".to_owned(),
            ));
        }
        if !bucket
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(Error::InvalidInput(
                "Bucket name must only contain alphanumeric characters, hyphens, or underscores"
                    .to_owned(),
            ));
        }
        if bucket.starts_with('-')
            || bucket.starts_with('_')
            || bucket.ends_with('-')
            || bucket.ends_with('_')
        {
            return Err(Error::InvalidInput(
                "Bucket name must not start or end with a hyphen or underscore".to_owned(),
            ));
        }

        let now = Utc::now().naive_utc();
        Ok(Self {
            id: Uuid::new_v4(),
            r#type: cloud_provider,
            region,
            bucket,
            credentials,
            sts_role_arn,
            endpoint,
            created_at: now,
            updated_at: now,
        })
    }

    // Method to update existing StorageProfile with new values
    pub fn update(&mut self, region: Option<String>, bucket: Option<String>) {
        if let Some(new_region) = region {
            self.region = new_region;
        }
        if let Some(new_bucket) = bucket {
            if new_bucket.len() >= 6 && new_bucket.len() <= 63 {
                self.bucket = new_bucket;
            }
        }
        self.updated_at = Utc::now().naive_utc();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct WarehouseCreateRequest {
    pub prefix: String,
    pub name: String,
    pub storage_profile_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Warehouse {
    pub id: Uuid,
    pub prefix: String,
    pub name: String,
    pub location: String,
    pub storage_profile_id: Uuid,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl Warehouse {
    pub fn new(prefix: String, name: String, storage_profile_id: Uuid) -> Result<Self, Error> {
        let id = Uuid::new_v4();
        let location = format!("{prefix}/{id}");
        let now = Utc::now().naive_utc();
        Ok(Self {
            id: Uuid::new_v4(),
            prefix,
            name,
            location,
            storage_profile_id,
            created_at: now,
            updated_at: now,
        })
    }
}

impl TryFrom<WarehouseCreateRequest> for Warehouse {
    type Error = Error;

    fn try_from(value: WarehouseCreateRequest) -> Result<Self, Self::Error> {
        Warehouse::new(
            value.prefix.clone(),
            value.name.clone(),
            value.storage_profile_id,
        )
    }
}

impl TryFrom<&WarehouseCreateRequest> for Warehouse {
    type Error = Error;

    fn try_from(value: &WarehouseCreateRequest) -> Result<Self, Self::Error> {
        Warehouse::new(
            value.prefix.clone(),
            value.name.clone(),
            value.storage_profile_id,
        )
    }
}
