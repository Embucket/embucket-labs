use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field};
use chrono::{NaiveDateTime, Utc};
use dotenv::dotenv;
use iceberg_rust::object_store::ObjectStoreBuilder;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::env;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

pub mod error;
pub use error::{ControlPlaneModelError, ControlPlaneModelResult};

// Enum for supported cloud providers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
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
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, strum::Display)]
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
    type Error = ControlPlaneModelError;

    fn try_from(value: &StorageProfileCreateRequest) -> ControlPlaneModelResult<Self> {
        Self::new(
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
    /// Creates a new `StorageProfile` with validation.
    ///
    /// # Errors
    ///
    /// Returns an `ControlPlaneModelError::InvalidInput` if:
    /// - Bucket name length is less than 6 or greater than 63 characters
    /// - Bucket name contains non-alphanumeric characters (other than hyphens or underscores)
    /// - Bucket name starts or ends with a hyphen or underscore
    pub fn new(
        cloud_provider: CloudProvider,
        region: String,
        bucket: String,
        credentials: Credentials,
        sts_role_arn: Option<String>,
        endpoint: Option<String>,
    ) -> ControlPlaneModelResult<Self> {
        // Example validation: Ensure bucket name length
        if bucket.len() < 6 || bucket.len() > 63 {
            return Err(ControlPlaneModelError::InvalidBucketName {
                bucket_name: bucket,
                reason: "Bucket name must be between 6 and 63 characters".to_owned(),
            });
        }
        if !bucket
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(ControlPlaneModelError::InvalidBucketName {
                bucket_name: bucket,
                reason:
                    "Bucket name must only contain alphanumeric characters, hyphens, or underscores"
                        .to_owned(),
            });
        }
        if bucket.starts_with('-')
            || bucket.starts_with('_')
            || bucket.ends_with('-')
            || bucket.ends_with('_')
        {
            return Err(ControlPlaneModelError::InvalidBucketName {
                bucket_name: bucket,
                reason: "Bucket name must not start or end with a hyphen or underscore".to_owned(),
            });
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

    /// Returns the get base url of this [`StorageProfile`].
    ///
    /// # Errors
    ///
    /// This function will return an error if the cloud platform isn't supported.
    pub fn get_base_url(&self) -> ControlPlaneModelResult<String> {
        // Doing this for every call is not efficient
        dotenv().ok();
        let use_file_system_instead_of_cloud = env::var("USE_FILE_SYSTEM_INSTEAD_OF_CLOUD")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .map_err(
                |e| error::ControlPlaneModelError::UnableToParseConfiguration {
                    key: "USE_FILE_SYSTEM_INSTEAD_OF_CLOUD".to_string(),
                    source: Box::new(e),
                },
            )?;
        if use_file_system_instead_of_cloud {
            let current_directory = env::current_dir()
                .map_err(|_| ControlPlaneModelError::InvalidDirectory {
                    directory: ".".to_string(),
                })
                .and_then(|cd| {
                    cd.to_str()
                        .map(String::from)
                        .ok_or(ControlPlaneModelError::InvalidDirectory {
                            directory: ".".to_string(),
                        })
                })?;
            Ok(format!("file://{current_directory}"))
        } else {
            match self.r#type {
                CloudProvider::AWS => Ok(format!("s3://{}", &self.bucket)),
                CloudProvider::AZURE => Err(ControlPlaneModelError::CloudProviderNotImplemented {
                    provider: "Azure".to_string(),
                }),
                CloudProvider::GCS => Err(ControlPlaneModelError::CloudProviderNotImplemented {
                    provider: "GCS".to_string(),
                }),
            }
        }
    }

    pub fn get_object_store_endpoint_url(&self) -> ControlPlaneModelResult<Url> {
        let storage_endpoint_url = self.endpoint.clone().unwrap_or_default();
        Url::parse(storage_endpoint_url.as_str()).context(error::InvalidEndpointUrlSnafu {
            url: storage_endpoint_url,
        })
    }

    // This is needed to initialize the catalog used in JanKaul code
    pub fn get_object_store_builder(&self) -> ControlPlaneModelResult<ObjectStoreBuilder> {
        // TODO remove duplicated code
        dotenv().ok();
        let use_file_system_instead_of_cloud = env::var("USE_FILE_SYSTEM_INSTEAD_OF_CLOUD")
            .context(error::MissingEnvironmentVariableSnafu {
                var: "USE_FILE_SYSTEM_INSTEAD_OF_CLOUD".to_string(),
            })?
            .parse::<bool>()
            .map_err(
                |e| error::ControlPlaneModelError::UnableToParseConfiguration {
                    key: "USE_FILE_SYSTEM_INSTEAD_OF_CLOUD".to_string(),
                    source: Box::new(e),
                },
            )?;
        if use_file_system_instead_of_cloud {
            // Here we initialise filesystem object store without root directory, because this code is used
            // by our catalog when we read metadata from the table - paths are absolute
            // In get_object_store function we are using the root directory
            Ok(ObjectStoreBuilder::Filesystem(Arc::new(
                LocalFileSystem::new(),
            )))
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
                        Credentials::AccessKey(creds) => Ok(ObjectStoreBuilder::S3(
                            builder
                                .with_access_key_id(&creds.aws_access_key_id)
                                .with_secret_access_key(&creds.aws_secret_access_key),
                        )),
                        Credentials::Role(_) => {
                            Err(error::ControlPlaneModelError::RoleBasedCredentialsNotSupported)
                        }
                    }
                }
                CloudProvider::AZURE | CloudProvider::GCS => {
                    Err(error::ControlPlaneModelError::CloudProviderNotImplemented {
                        provider: self.r#type.to_string(),
                    })
                }
            }
        }
    }

    pub fn get_object_store(&self) -> ControlPlaneModelResult<Box<dyn ObjectStore>> {
        // TODO remove duplicated code
        dotenv().ok();
        let use_file_system_instead_of_cloud = env::var("USE_FILE_SYSTEM_INSTEAD_OF_CLOUD")
            .context(error::MissingEnvironmentVariableSnafu {
                var: "USE_FILE_SYSTEM_INSTEAD_OF_CLOUD".to_string(),
            })?
            .parse::<bool>()
            .map_err(
                |e| error::ControlPlaneModelError::UnableToParseConfiguration {
                    key: "USE_FILE_SYSTEM_INSTEAD_OF_CLOUD".to_string(),
                    source: Box::new(e),
                },
            )?;
        if use_file_system_instead_of_cloud {
            // Here we initialise filesystem object store without current directory as root, because this code is used
            // by our catalog when we write metadata file - we use relative path
            // In get_object_store_builder function we are using absolute paths
            let lfs = LocalFileSystem::new_with_prefix(".").map_err(|_| {
                error::ControlPlaneModelError::InvalidDirectory {
                    directory: ".".to_string(),
                }
            })?;
            Ok(Box::new(lfs))
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
                        Credentials::AccessKey(creds) => Ok(Box::new(
                            builder
                                .with_access_key_id(&creds.aws_access_key_id)
                                .with_secret_access_key(&creds.aws_secret_access_key)
                                .build()
                                .context(error::ObjectStoreSnafu)?,
                        )),
                        Credentials::Role(_) => {
                            Err(error::ControlPlaneModelError::RoleBasedCredentialsNotSupported)
                        }
                    }
                }
                CloudProvider::AZURE | CloudProvider::GCS => {
                    Err(error::ControlPlaneModelError::CloudProviderNotImplemented {
                        provider: self.r#type.to_string(),
                    })
                }
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
    pub fn new(
        prefix: String,
        name: String,
        storage_profile_id: Uuid,
    ) -> ControlPlaneModelResult<Self> {
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
    type Error = ControlPlaneModelError;

    fn try_from(value: WarehouseCreateRequest) -> ControlPlaneModelResult<Self> {
        Self::new(
            value.prefix.clone(),
            value.name.clone(),
            value.storage_profile_id,
        )
    }
}

impl TryFrom<&WarehouseCreateRequest> for Warehouse {
    type Error = ControlPlaneModelError;

    fn try_from(value: &WarehouseCreateRequest) -> ControlPlaneModelResult<Self> {
        Self::new(
            value.prefix.clone(),
            value.name.clone(),
            value.storage_profile_id,
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub nullable: bool,
    pub r#type: String,
    pub byte_length: Option<i32>,
    pub length: Option<i32>,
    pub scale: Option<i32>,
    pub precision: Option<i32>,
    pub collation: Option<String>,
}

impl ColumnInfo {
    #[must_use]
    pub fn to_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("logicalType".to_string(), self.r#type.to_uppercase());
        metadata.insert(
            "precision".to_string(),
            self.precision.unwrap_or(38).to_string(),
        );
        metadata.insert("scale".to_string(), self.scale.unwrap_or(0).to_string());
        metadata.insert(
            "charLength".to_string(),
            self.length.unwrap_or(0).to_string(),
        );
        metadata
    }

    #[must_use]
    pub fn from_batch(records: &[RecordBatch]) -> Vec<Self> {
        let mut column_infos = Vec::new();

        if records.is_empty() {
            return column_infos;
        }
        for field in records[0].schema().fields() {
            column_infos.push(Self::from_field(field));
        }
        column_infos
    }

    #[must_use]
    pub fn from_field(field: &Field) -> Self {
        let mut column_info = Self {
            name: field.name().clone(),
            database: String::new(), // TODO
            schema: String::new(),   // TODO
            table: String::new(),    // TODO
            nullable: field.is_nullable(),
            r#type: field.data_type().to_string(),
            byte_length: None,
            length: None,
            scale: None,
            precision: None,
            collation: None,
        };

        match field.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                column_info.r#type = "fixed".to_string();
                column_info.precision = Some(38);
                column_info.scale = Some(0);
            }
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                column_info.r#type = "fixed".to_string();
                column_info.precision = Some(i32::from(*precision));
                column_info.scale = Some(i32::from(*scale));
            }
            DataType::Boolean => {
                column_info.r#type = "boolean".to_string();
            }
            // Varchar, Char, Utf8
            DataType::Utf8 => {
                column_info.r#type = "text".to_string();
                column_info.byte_length = Some(16_777_216);
                column_info.length = Some(16_777_216);
            }
            DataType::Time32(_) | DataType::Time64(_) => {
                column_info.r#type = "time".to_string();
                column_info.precision = Some(0);
                column_info.scale = Some(9);
            }
            DataType::Date32 | DataType::Date64 => {
                column_info.r#type = "date".to_string();
            }
            DataType::Timestamp(_, _) => {
                column_info.r#type = "timestamp_ntz".to_string();
                column_info.precision = Some(0);
                column_info.scale = Some(9);
            }
            DataType::Binary => {
                column_info.r#type = "binary".to_string();
                column_info.byte_length = Some(8_388_608);
                column_info.length = Some(8_388_608);
            }
            _ => {}
        }
        column_info
    }
}
