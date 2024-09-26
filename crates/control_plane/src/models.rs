use chrono::{NaiveDateTime, Utc};
use uuid::Uuid;
use std::convert::TryFrom;
use crate::error::Error;

// Enum for supported cloud providers
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudProvider {
    AWS,
    AZURE,
    GCS,
}

// AWS Access Key Credentials
#[derive(Debug, Clone)]
pub struct AwsAccessKeyCredential {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,  // Business model may not use SecretStr, it's presentation-specific
}

// AWS Role Credentials
#[derive(Debug, Clone)]
pub struct AwsRoleCredential {
    pub role_arn: String,
    pub external_id: String,
}

// Composite enum for credentials
#[derive(Debug, Clone)]
pub enum Credentials {
    AccessKey(AwsAccessKeyCredential),
    Role(AwsRoleCredential),
}

// Core StorageProfile structure in the business model
#[derive(Debug, Clone)]
pub struct StorageProfile {
    pub id: Uuid,
    pub cloud_provider: CloudProvider,
    pub region: String,
    pub bucket: String,
    pub credentials: Credentials,
    pub sts_role_arn: Option<String>,
    pub endpoint: Option<String>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone)]
pub struct StorageProfileCreateRequest {
    pub cloud_provider: CloudProvider,
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
            value.cloud_provider,
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
            value.cloud_provider,
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
            return Err(Error::InvalidInput("Bucket name must be between 6 and 63 characters".to_owned()));
        }
        if !bucket.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            return Err(Error::InvalidInput("Bucket name must only contain alphanumeric characters, hyphens, or underscores".to_owned()));
        }
        if bucket.starts_with('-') || bucket.starts_with('_') || bucket.ends_with('-') || bucket.ends_with('_') {
            return Err(Error::InvalidInput("Bucket name must not start or end with a hyphen or underscore".to_owned()));
        }
        
        let now = Utc::now().naive_utc();
        Ok(Self {
            id: Uuid::new_v4(),
            cloud_provider,
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