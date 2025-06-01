use crate::error::{self as metastore_error, MetastoreResult};
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::fmt::Display;
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
    pub aws_access_key_id: String,
    #[validate(length(min = 1))]
    pub aws_secret_access_key: String,
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
pub struct S3Volume {
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

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct S3TablesVolume {
    #[validate(length(min = 1))]
    pub region: String,
    #[validate(length(min = 1), custom(function = "validate_bucket_name"))]
    pub bucket: Option<String>,
    #[validate(length(min = 1))]
    pub endpoint: String,
    #[validate(nested)]
    pub credentials: AwsCredentials,
    #[validate(length(min = 1), custom(function = "validate_bucket_name"))]
    pub name: String,
    #[validate(length(min = 1))]
    pub arn: String,
}

impl S3TablesVolume {
    #[must_use]
    pub fn s3_builder(&self) -> AmazonS3Builder {
        let s3_volume = S3Volume {
            region: Some(self.region.clone()),
            bucket: Some(self.name.clone()),
            endpoint: Some(self.endpoint.clone()),
            skip_signature: None,
            metadata_endpoint: None,
            credentials: Some(self.credentials.clone()),
        };
        Volume::get_s3_builder(&s3_volume)
    }
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
pub struct FileVolume {
    #[validate(length(min = 1))]
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum VolumeType {
    S3(S3Volume),
    S3Tables(S3TablesVolume),
    File(FileVolume),
    Memory,
}

impl Display for VolumeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::S3(_) => write!(f, "s3"),
            Self::S3Tables(_) => write!(f, "s3_tables"),
            Self::File(_) => write!(f, "file"),
            Self::Memory => write!(f, "memory"),
        }
    }
}

impl Validate for VolumeType {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            Self::S3(volume) => volume.validate(),
            Self::S3Tables(volume) => volume.validate(),
            Self::File(volume) => volume.validate(),
            Self::Memory => Ok(()),
        }
    }
}

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct Volume {
    pub ident: VolumeIdent,
    #[serde(flatten)]
    #[validate(nested)]
    pub volume: VolumeType,
}

pub type VolumeIdent = String;

#[allow(clippy::as_conversions)]
impl Volume {
    #[must_use]
    pub const fn new(ident: VolumeIdent, volume: VolumeType) -> Self {
        Self { ident, volume }
    }

    pub fn get_object_store(&self) -> MetastoreResult<Arc<dyn ObjectStore>> {
        match &self.volume {
            VolumeType::S3(volume) => {
                let s3_builder = Self::get_s3_builder(volume);
                s3_builder
                    .build()
                    .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                    .context(metastore_error::ObjectStoreSnafu)
                    .map_err(Box::new)
            }
            VolumeType::S3Tables(volume) => {
                let s3_builder = volume.s3_builder();
                s3_builder
                    .build()
                    .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                    .context(metastore_error::ObjectStoreSnafu)
                    .map_err(Box::new)
            }
            VolumeType::File(_) => Ok(Arc::new(
                object_store::local::LocalFileSystem::new().with_automatic_cleanup(true),
            ) as Arc<dyn ObjectStore>),
            VolumeType::Memory => {
                Ok(Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>)
            }
        }
    }

    #[must_use]
    pub fn get_s3_builder(volume: &S3Volume) -> AmazonS3Builder {
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
                    s3_builder = s3_builder.with_access_key_id(creds.aws_access_key_id.clone());
                    s3_builder =
                        s3_builder.with_secret_access_key(creds.aws_secret_access_key.clone());
                }
                AwsCredentials::Token(token) => {
                    s3_builder = s3_builder.with_token(token.clone());
                }
            }
        }
        s3_builder
    }

    #[must_use]
    pub fn prefix(&self) -> String {
        match &self.volume {
            VolumeType::S3(volume) => volume
                .bucket
                .as_ref()
                .map_or_else(|| "s3://".to_string(), |bucket| format!("s3://{bucket}")),
            VolumeType::S3Tables(volume) => volume
                .bucket
                .as_ref()
                .map_or_else(|| "s3://".to_string(), |bucket| format!("s3://{bucket}")),
            VolumeType::File(volume) => format!("file://{}", volume.path),
            VolumeType::Memory => "memory://".to_string(),
        }
    }

    pub async fn validate_credentials(&self) -> MetastoreResult<()> {
        let object_store = self.get_object_store()?;
        object_store
            .get(&Path::from(self.prefix()))
            .await
            .context(metastore_error::ObjectStoreSnafu)
            .map_err(Box::new)?;
        Ok(())
    }
}
