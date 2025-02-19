// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field};
use chrono::{NaiveDateTime, Utc};
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
    FS,
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
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub credentials: Option<Credentials>,
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
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub credentials: Option<Credentials>,
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
        region: Option<String>,
        bucket: Option<String>,
        credentials: Option<Credentials>,
        sts_role_arn: Option<String>,
        endpoint: Option<String>,
    ) -> ControlPlaneModelResult<Self> {
        match cloud_provider {
            CloudProvider::AWS | CloudProvider::AZURE | CloudProvider::GCS => {
                if credentials.is_none() {
                    return Err(ControlPlaneModelError::MissingCredentials {
                        profile_type: cloud_provider.to_string(),
                    });
                }

                if region.is_none() {
                    return Err(ControlPlaneModelError::InvalidRegionName {
                        region: String::new(),
                        reason: "Region name is required".to_owned(),
                    });
                }

                if bucket.is_none() {
                    return Err(ControlPlaneModelError::InvalidBucketName {
                        bucket_name: String::new(),
                        reason: "Bucket name is required".to_owned(),
                    });
                }

                if let Some(b) = &bucket {
                    if b.len() < 6 || b.len() > 63 {
                        return Err(ControlPlaneModelError::InvalidBucketName {
                            bucket_name: b.clone(),
                            reason: "Bucket name must be between 6 and 63 characters".to_owned(),
                        });
                    }
                    if !b
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
                    {
                        return Err(ControlPlaneModelError::InvalidBucketName {
                            bucket_name: b.clone(),
                            reason:
                            "Bucket name must only contain alphanumeric characters, hyphens, or underscores"
                                .to_owned(),
                        });
                    }
                    if b.starts_with('-')
                        || b.starts_with('_')
                        || b.ends_with('-')
                        || b.ends_with('_')
                    {
                        return Err(ControlPlaneModelError::InvalidBucketName {
                            bucket_name: b.clone(),
                            reason: "Bucket name must not start or end with a hyphen or underscore"
                                .to_owned(),
                        });
                    }
                }
            }
            CloudProvider::FS => {}
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
        self.region = region;
        if let Some(ref new_bucket) = bucket {
            if new_bucket.len() >= 6 && new_bucket.len() <= 63 {
                self.bucket = bucket;
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
        match self.r#type {
            CloudProvider::AWS => Ok(format!("s3://{}", &self.bucket.clone().unwrap_or_default())),
            CloudProvider::AZURE => Err(ControlPlaneModelError::CloudProviderNotImplemented {
                provider: "Azure".to_string(),
            }),
            CloudProvider::GCS => Err(ControlPlaneModelError::CloudProviderNotImplemented {
                provider: "GCS".to_string(),
            }),
            CloudProvider::FS => {
                let current_directory = env::current_dir()
                    .map_err(|_| ControlPlaneModelError::InvalidDirectory {
                        directory: ".".to_string(),
                    })
                    .and_then(|cd| {
                        cd.to_str().map(String::from).ok_or(
                            ControlPlaneModelError::InvalidDirectory {
                                directory: ".".to_string(),
                            },
                        )
                    })?;
                Ok(format!("file://{current_directory}"))
            }
        }
    }

    pub fn get_object_store_endpoint_url(&self) -> ControlPlaneModelResult<Url> {
        let storage_endpoint_url = match self.r#type {
            CloudProvider::FS => "file://".to_string(),
            _ => self.get_base_url()?,
        };
        Url::parse(storage_endpoint_url.as_str()).context(error::InvalidEndpointUrlSnafu {
            url: storage_endpoint_url,
        })
    }

    pub fn get_s3_builder(&self) -> ControlPlaneModelResult<AmazonS3Builder> {
        let mut builder = AmazonS3Builder::new()
            .with_region(self.region.clone().unwrap_or_default())
            .with_bucket_name(self.bucket.clone().unwrap_or_default());

        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint.clone());
        }

        if let Some(credentials) = &self.credentials {
            match credentials {
                Credentials::AccessKey(creds) => {
                    builder = builder
                        .with_access_key_id(&creds.aws_access_key_id)
                        .with_secret_access_key(&creds.aws_secret_access_key);
                }
                Credentials::Role(_) => {
                    return Err(ControlPlaneModelError::RoleBasedCredentialsNotSupported);
                }
            }
        } else {
            return Err(ControlPlaneModelError::MissingCredentials {
                profile_type: self.r#type.to_string(),
            });
        }
        Ok(builder)
    }

    // This is needed to initialize the catalog used in JanKaul code
    pub fn get_object_store_builder(&self) -> ControlPlaneModelResult<ObjectStoreBuilder> {
        match self.r#type {
            CloudProvider::FS => Ok(ObjectStoreBuilder::Filesystem(Arc::new(
                LocalFileSystem::new(),
            ))),
            CloudProvider::AWS => Ok(ObjectStoreBuilder::S3(self.get_s3_builder()?)),
            CloudProvider::AZURE | CloudProvider::GCS => {
                Err(ControlPlaneModelError::CloudProviderNotImplemented {
                    provider: self.r#type.to_string(),
                })
            }
        }
    }

    pub fn get_object_store(&self) -> ControlPlaneModelResult<Box<dyn ObjectStore>> {
        match self.r#type {
            CloudProvider::FS => {
                // Here we initialise filesystem object store without current directory as root, because this code is used
                // by our catalog when we write metadata file - we use relative path
                // In get_object_store_builder function we are using absolute paths
                let lfs = LocalFileSystem::new_with_prefix(".").map_err(|_| {
                    ControlPlaneModelError::InvalidDirectory {
                        directory: ".".to_string(),
                    }
                })?;
                Ok(Box::new(lfs))
            }
            CloudProvider::AWS => Ok(Box::new(
                self.get_s3_builder()?
                    .build()
                    .context(error::ObjectStoreSnafu)?,
            )),
            CloudProvider::AZURE | CloudProvider::GCS => {
                Err(ControlPlaneModelError::CloudProviderNotImplemented {
                    provider: self.r#type.to_string(),
                })
            }
        }
    }
}

impl Default for StorageProfile {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            r#type: CloudProvider::FS,
            region: None,
            bucket: None,
            credentials: None,
            sts_role_arn: None,
            endpoint: None,
            created_at: Utc::now().naive_utc(),
            updated_at: Utc::now().naive_utc(),
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
            | DataType::UInt64
            | DataType::Float32 => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    #[allow(clippy::unwrap_used)]
    fn create_dummy_profile() -> StorageProfile {
        StorageProfile::new(
            CloudProvider::AWS,
            Some("us-west-1".to_string()),
            Some("bucket".to_string()),
            Some(Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "access_key".to_string(),
                aws_secret_access_key: "secret_key".to_string(),
            })),
            None,
            None,
        )
        .unwrap()
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_new_storage_profile_with_validation() {
        let test_cases = vec![
            (
                CloudProvider::AWS,
                None,
                None,
                None,
                Err(ControlPlaneModelError::MissingCredentials {
                    profile_type: "aws".to_string(),
                }),
            ),
            (
                CloudProvider::AWS,
                None,
                None,
                Some(Credentials::AccessKey(AwsAccessKeyCredential {
                    aws_access_key_id: "test_key".to_string(),
                    aws_secret_access_key: "test_secret".to_string(),
                })),
                Err(ControlPlaneModelError::InvalidRegionName {
                    region: String::new(),
                    reason: "Region name is required".to_owned(),
                }),
            ),
            (
                CloudProvider::AWS,
                Some("us-west-1".to_string()),
                None,
                Some(Credentials::AccessKey(AwsAccessKeyCredential {
                    aws_access_key_id: "test_key".to_string(),
                    aws_secret_access_key: "test_secret".to_string(),
                })),
                Err(ControlPlaneModelError::InvalidBucketName {
                    bucket_name: String::new(),
                    reason: "Bucket name is required".to_owned(),
                }),
            ),
            (
                CloudProvider::AWS,
                Some("us-west-1".to_string()),
                Some("short".to_string()),
                None,
                Err(ControlPlaneModelError::InvalidBucketName {
                    bucket_name: "us".to_string(),
                    reason: "Bucket name must be between 6 and 63 characters".to_owned(),
                }),
            ),
            (
                CloudProvider::AWS,
                Some("us-west-1".to_string()),
                Some("us!!_".to_string()),
                None,
                Err(ControlPlaneModelError::InvalidBucketName {
                    bucket_name: "us!!_".to_string(),
                    reason:
                    "Bucket name must only contain alphanumeric characters, hyphens, or underscores"
                        .to_owned(),
                }),
            ),
            (
                CloudProvider::AWS,
                Some("us-west-1".to_string()),
                Some("_invalid-bucket".to_string()),
                Some(Credentials::AccessKey(AwsAccessKeyCredential {
                    aws_access_key_id: "test_key".to_string(),
                    aws_secret_access_key: "test_secret".to_string(),
                })),
                Err(ControlPlaneModelError::InvalidBucketName {
                    bucket_name: "_invalid-bucket".to_string(),
                    reason: "Bucket name must not start or end with a hyphen or underscore"
                        .to_owned(),
                }),
            ),
            (
                CloudProvider::AWS,
                Some("us-west-1".to_string()),
                Some("valid-bucket".to_string()),
                Some(Credentials::AccessKey(AwsAccessKeyCredential {
                    aws_access_key_id: "test_key".to_string(),
                    aws_secret_access_key: "test_secret".to_string(),
                })),
                Ok(()),
            ),
            (
                CloudProvider::FS,
                None,
                None,
                None,
                Ok(()),
            ),
        ];
        for (cloud_provider, region, bucket, credentials, expected) in test_cases {
            let result = StorageProfile::new(
                cloud_provider,
                region.clone(),
                bucket.clone(),
                credentials.clone(),
                None,
                None,
            );
            assert_eq!(result.is_ok(), expected.is_ok());
        }
    }

    #[tokio::test]
    async fn test_update_storage_profile() {
        let mut storage_profile = StorageProfile::default();
        storage_profile.update(
            Some("us-west-1".to_string()),
            Some("new-bucket".to_string()),
        );
        assert_eq!(storage_profile.region, Some("us-west-1".to_string()));
        assert_eq!(storage_profile.bucket, Some("new-bucket".to_string()));

        storage_profile.update(Some("us-west-1".to_string()), Some("new".to_string()));
        assert_eq!(storage_profile.bucket, Some("new-bucket".to_string()));
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_storage_profile_get_base_url() {
        let mut sp = create_dummy_profile();
        let base_url = sp.get_base_url().unwrap();
        assert_eq!(base_url, "s3://bucket");

        sp.r#type = CloudProvider::AZURE;
        let base_url = sp.get_base_url();
        assert!(base_url.is_err());

        sp.r#type = CloudProvider::GCS;
        let base_url = sp.get_base_url();
        assert!(base_url.is_err());

        sp.r#type = CloudProvider::FS;
        let base_url = sp.get_base_url().unwrap();
        assert!(base_url.starts_with("file://"));
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_storage_profile_get_object_store_endpoint_url() {
        let sp = create_dummy_profile();
        let endpoint_url = sp.get_object_store_endpoint_url().unwrap();
        assert_eq!(endpoint_url.as_str(), "s3://bucket");

        let sp = StorageProfile::default();
        let endpoint_url = sp.get_object_store_endpoint_url().unwrap();
        assert_eq!(endpoint_url.as_str(), "file:///");
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_column_info_from_field() {
        let field = Field::new("test_field", DataType::Int8, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "fixed");
        assert!(!column_info.nullable);

        let field = Field::new("test_field", DataType::Decimal128(1, 2), true);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "fixed");
        assert_eq!(column_info.precision.unwrap(), 1);
        assert_eq!(column_info.scale.unwrap(), 2);
        assert!(column_info.nullable);

        let field = Field::new("test_field", DataType::Boolean, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "boolean");

        let field = Field::new("test_field", DataType::Time32(TimeUnit::Second), false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "time");
        assert_eq!(column_info.precision.unwrap(), 0);
        assert_eq!(column_info.scale.unwrap(), 9);

        let field = Field::new("test_field", DataType::Date32, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "date");

        let field = Field::new(
            "test_field",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        );
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "timestamp_ntz");
        assert_eq!(column_info.precision.unwrap(), 0);
        assert_eq!(column_info.scale.unwrap(), 9);

        let field = Field::new("test_field", DataType::Binary, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "binary");
        assert_eq!(column_info.byte_length.unwrap(), 8_388_608);
        assert_eq!(column_info.length.unwrap(), 8_388_608);
    }

    #[tokio::test]
    async fn test_to_metadata() {
        let column_info = ColumnInfo {
            name: "test_field".to_string(),
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            nullable: false,
            r#type: "fixed".to_string(),
            byte_length: Some(8_388_608),
            length: Some(8_388_608),
            scale: Some(0),
            precision: Some(38),
            collation: None,
        };
        let metadata = column_info.to_metadata();
        assert_eq!(metadata.get("logicalType"), Some(&"FIXED".to_string()));
        assert_eq!(metadata.get("precision"), Some(&"38".to_string()));
        assert_eq!(metadata.get("scale"), Some(&"0".to_string()));
        assert_eq!(metadata.get("charLength"), Some(&"8388608".to_string()));
    }
}
