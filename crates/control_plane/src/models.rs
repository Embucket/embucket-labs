use chrono::{NaiveDateTime, Utc};
use uuid::Uuid;

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

impl From<StorageProfileCreateRequest> for StorageProfile {
    fn from(params: StorageProfileCreateRequest) -> Self {
        let now = Utc::now().naive_utc();
        Self {
            id: Uuid::new_v4(),
            cloud_provider: params.cloud_provider,
            region: params.region,
            bucket: params.bucket,
            credentials: params.credentials,
            sts_role_arn: params.sts_role_arn,
            endpoint: params.endpoint,
            created_at: now,
            updated_at: now,
        }
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
    ) -> Result<Self, &'static str> {
        // Example validation: Ensure bucket name length
        if bucket.len() < 6 || bucket.len() > 63 {
            return Err("Bucket name must be between 6 and 63 characters");
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