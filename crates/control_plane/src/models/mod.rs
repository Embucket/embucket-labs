use crate::error::Error;
use chrono::{NaiveDateTime, Utc};
use dotenv::dotenv;
use iceberg_rust::catalog::bucket::ObjectStoreBuilder;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::env;
use std::sync::Arc;
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
    pub validate_credentials: Option<bool>,
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

    pub fn get_base_url(&self) -> String {
        dotenv().ok();
        let use_file_system_instead_of_cloud = env::var("USE_FILE_SYSTEM_INSTEAD_OF_CLOUD").unwrap_or("true".to_string())
            .parse::<bool>()
            .expect
            ("Failed to parse \
            USE_FILE_SYSTEM_INSTEAD_OF_CLOUD");
        if use_file_system_instead_of_cloud {
            format!("file://{}", env::current_dir().unwrap().to_str().unwrap())
        } else {
            match self.r#type {
                CloudProvider::AWS => {
                    format!("s3://{}", &self.bucket)
                }
                CloudProvider::AZURE => { panic!("Not implemented") }
                CloudProvider::GCS => { panic!("Not implemented") }
            }
        }
    }

    // This is needed to initialize the catalog used in JanKaul code
    pub fn get_object_store_builder(&self) -> ObjectStoreBuilder {
        // TODO remove duplicated code
        dotenv().ok();
        let use_file_system_instead_of_cloud = env::var("USE_FILE_SYSTEM_INSTEAD_OF_CLOUD").ok().unwrap()
            .parse::<bool>()
            .expect
            ("Failed to parse \
            USE_FILE_SYSTEM_INSTEAD_OF_CLOUD");
        if use_file_system_instead_of_cloud {
            // Here we initialise filesystem object store without root directory, because this code is used
            // by our catalog when we read metadata from the table - paths are absolute
            // In get_object_store function we are using the root directory
            ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new()))
        } else {
            match self.r#type {
                CloudProvider::AWS => {
                    let mut builder = AmazonS3Builder::new()
                        .with_region(&self.region)
                        .with_bucket_name(&self.bucket)
                        .with_allow_http(true); // TODO should be only the case for local development
                    builder = if let Some(endpoint) = &self.endpoint {
                        builder.with_endpoint(endpoint.clone())
                    } else {
                        builder
                    };
                    match &self.credentials {
                        Credentials::AccessKey(creds) => {
                            ObjectStoreBuilder::S3(builder.with_access_key_id(&creds.aws_access_key_id)
                                .with_secret_access_key(&creds.aws_secret_access_key))
                        }
                        Credentials::Role(_) => {
                            panic!("Not implemented")
                        }
                    }
                }
                CloudProvider::AZURE => { panic!("Not implemented") }
                CloudProvider::GCS => { panic!("Not implemented") }
            }
        }
    }

    pub fn get_object_store(&self) -> Box<dyn ObjectStore> {
        // TODO remove duplicated code
        dotenv().ok();
        let use_file_system_instead_of_cloud = env::var("USE_FILE_SYSTEM_INSTEAD_OF_CLOUD").ok().unwrap()
            .parse::<bool>()
            .expect
            ("Failed to parse \
            USE_FILE_SYSTEM_INSTEAD_OF_CLOUD");
        if use_file_system_instead_of_cloud {
            // Here we initialise filesystem object store without current directory as root, because this code is used
            // by our catalog when we write metadata file - we use relative path
            // In get_object_store_builder function we are using absolute paths
            Box::new(LocalFileSystem::new_with_prefix(".").unwrap())
        } else {
            match self.r#type {
                CloudProvider::AWS => {
                    let mut builder = AmazonS3Builder::new()
                        .with_region(&self.region)
                        .with_bucket_name(&self.bucket)
                        .with_allow_http(true); // TODO should be only the case for local development
                    builder = if let Some(endpoint) = &self.endpoint {
                        builder.with_endpoint(endpoint.clone())
                    } else {
                        builder
                    };
                    match &self.credentials {
                        Credentials::AccessKey(creds) => {
                            Box::new(builder.with_access_key_id(&creds.aws_access_key_id)
                                .with_secret_access_key(&creds.aws_secret_access_key)
                                .build()
                                .expect("error creating AWS object store"))
                        }
                        Credentials::Role(_) => {
                            panic!("Not implemented")
                        }
                    }
                }
                CloudProvider::AZURE => { panic!("Not implemented") }
                CloudProvider::GCS => { panic!("Not implemented") }
            }
        }
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
            id,
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
