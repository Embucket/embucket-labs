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

use crate::error::{self as metastore_error, MetastoreResult};
use object_store::{aws::AmazonS3Builder, path::Path, ObjectStore};
use serde::{Deserialize, Serialize, Serializer};
use snafu::ResultExt;
use std::sync::Arc;
use validator::{Validate, ValidationError, ValidationErrors};

// Enum for supported cloud providers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
pub enum CloudProvider {
    AWS,
    AZURE,
    GCS,
    FS,
    MEMORY,
}

// AWS Access Key Credentials
#[derive(Validate, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct AwsAccessKeyCredentials {
    #[validate(length(min = 1))]
    #[serde(serialize_with = "hide_sensitive")]
    pub aws_access_key_id: String,
    #[validate(length(min = 1))]
    #[serde(serialize_with = "hide_sensitive")]
    pub aws_secret_access_key: String,
}

fn hide_sensitive<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("********")
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, utoipa::ToSchema)]
#[serde(tag = "credential_type", rename_all = "kebab-case")]
pub enum AwsCredentials {
    #[serde(rename = "access_key")]
    AccessKey(AwsAccessKeyCredentials),
    #[serde(rename = "token")]
    Token(String),
}

impl Validate for AwsCredentials {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            Self::AccessKey(creds) => creds.validate(),
            Self::Token(token) => {
                if token.is_empty() {
                    let mut errors = ValidationErrors::new();
                    errors.add("token", ValidationError::new("Token must not be empty"));
                    return Err(errors);
                }
                Ok(())
            }
        }
    }
}

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct IceBucketS3Volume {
    #[validate(length(min = 1))]
    pub region: Option<String>,
    #[validate(length(min = 1), custom(function = "validate_bucket_name"))]
    pub bucket: Option<String>,
    #[validate(length(min = 1))]
    pub endpoint: Option<String>,
    pub skip_signature: Option<bool>,
    #[validate(length(min = 1))]
    pub metadata_endpoint: Option<String>,
    #[validate(required, nested)]
    pub credentials: Option<AwsCredentials>,
}

fn validate_bucket_name(bucket_name: &str) -> Result<(), ValidationError> {
    if !bucket_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(ValidationError::new(
            "Bucket name must only contain alphanumeric characters, hyphens, or underscores",
        ));
    }
    if bucket_name.starts_with('-')
        || bucket_name.starts_with('_')
        || bucket_name.ends_with('-')
        || bucket_name.ends_with('_')
    {
        return Err(ValidationError::new(
            "Bucket name must not start or end with a hyphen or underscore",
        ));
    }
    Ok(())
}

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct IceBucketFileVolume {
    #[validate(length(min = 1))]
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum IceBucketVolumeType {
    S3(IceBucketS3Volume),
    File(IceBucketFileVolume),
    Memory,
}

impl Validate for IceBucketVolumeType {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            Self::S3(volume) => volume.validate(),
            Self::File(volume) => volume.validate(),
            Self::Memory => Ok(()),
        }
    }
}

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct IceBucketVolume {
    pub ident: IceBucketVolumeIdent,
    #[serde(flatten)]
    #[validate(nested)]
    pub volume: IceBucketVolumeType,
}

pub type IceBucketVolumeIdent = String;

#[allow(clippy::as_conversions)]
impl IceBucketVolume {
    #[must_use]
    pub const fn new(ident: IceBucketVolumeIdent, volume: IceBucketVolumeType) -> Self {
        Self { ident, volume }
    }

    pub fn get_object_store(&self) -> MetastoreResult<Arc<dyn ObjectStore>> {
        match &self.volume {
            IceBucketVolumeType::S3(volume) => {
                let mut s3_builder = AmazonS3Builder::new()
                    .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);

                if let Some(region) = &volume.region {
                    s3_builder = s3_builder.with_region(region);
                }
                if let Some(bucket) = &volume.bucket {
                    s3_builder = s3_builder.with_bucket_name(bucket.clone());
                }
                if let Some(endpoint) = &volume.endpoint {
                    s3_builder = s3_builder.with_endpoint(endpoint);
                    s3_builder = s3_builder.with_allow_http(endpoint.starts_with("http:"));
                }
                if let Some(metadata_endpoint) = &volume.metadata_endpoint {
                    s3_builder = s3_builder.with_metadata_endpoint(metadata_endpoint);
                }
                if let Some(skip_signature) = volume.skip_signature {
                    s3_builder = s3_builder.with_skip_signature(skip_signature);
                }
                if let Some(credentials) = &volume.credentials {
                    match credentials {
                        AwsCredentials::AccessKey(creds) => {
                            s3_builder =
                                s3_builder.with_access_key_id(creds.aws_access_key_id.clone());
                            s3_builder = s3_builder
                                .with_secret_access_key(creds.aws_secret_access_key.clone());
                        }
                        AwsCredentials::Token(token) => {
                            s3_builder = s3_builder.with_token(token.clone());
                        }
                    }
                }
                s3_builder
                    .build()
                    .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                    .context(metastore_error::ObjectStoreSnafu)
            }
            IceBucketVolumeType::File(volume) => {
                Ok(Arc::new(object_store::local::LocalFileSystem::new()) as Arc<dyn ObjectStore>)
            }
            IceBucketVolumeType::Memory => {
                Ok(Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>)
            }
        }
    }

    #[must_use]
    pub fn prefix(&self) -> String {
        match &self.volume {
            IceBucketVolumeType::S3(volume) => volume
                .bucket
                .as_ref()
                .map_or_else(|| "s3://".to_string(), |bucket| format!("s3://{bucket}")),
            IceBucketVolumeType::File(volume) => format!("file://{}", volume.path),
            IceBucketVolumeType::Memory => "memory://".to_string(),
        }
    }

    pub async fn validate_credentials(&self) -> MetastoreResult<()> {
        let object_store = self.get_object_store()?;
        object_store
            .get(&Path::from(self.prefix()))
            .await
            .context(metastore_error::ObjectStoreSnafu)?;
        Ok(())
    }
}
