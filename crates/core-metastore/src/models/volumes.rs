use crate::error::{self as metastore_error, Result};
use super::{MAP_VOLUME_ID, NamedId, RwObject};
use object_store::{
    ClientOptions, ObjectStore,
    aws::{AmazonS3Builder, resolve_bucket_region},
    local::LocalFileSystem,
    path::Path,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::fmt::Display;
use std::sync::Arc;
use validator::{Validate, ValidationError, ValidationErrors};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeId(pub i64);

impl NamedId for VolumeId {
    fn type_name() -> &'static str {
        MAP_VOLUME_ID
    }
}

impl std::ops::Deref for VolumeId {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::from_over_into)]
impl Into<i64> for VolumeId {
    fn into(self) -> i64 {
        self.0
    }
}

impl RwObject<Volume> {
    #[must_use]
    pub fn with_id(self, id: VolumeId) -> Self {
        self.with_named_id(VolumeId::type_name(), *id)
    }

    pub fn id(&self) -> Result<VolumeId> {
        self.named_id(VolumeId::type_name()).map(VolumeId)
    }
}

// Enum for supported cloud providers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
pub enum CloudProvider {
    AWS,
    AZURE,
    GCS,
    FS,
    MEMORY,
}

#[allow(clippy::expect_used)]
fn aws_access_key_id_regex_func() -> Regex {
    Regex::new(r"^[a-zA-Z0-9]{20}$").expect("Failed to create aws_access_key_id_regex")
}

#[allow(clippy::expect_used)]
fn aws_secret_access_key_regex_func() -> Regex {
    Regex::new(r"^[A-Za-z0-9/+=]{40}$").expect("Failed to create aws_secret_access_key_regex")
}

#[allow(clippy::expect_used)]
fn s3_endpoint_regex_func() -> Regex {
    Regex::new(r"^https?://").expect("Failed to create s3_endpoint_regex")
}

#[allow(clippy::expect_used)]
fn s3tables_arn_regex_func() -> Regex {
    Regex::new(r"^arn:aws:s3tables:[a-z0-9-]+:\d+:bucket/[a-zA-Z0-9.\-_]+$")
        .expect("Failed to create s3tables_arn_regex")
}

// AWS Access Key Credentials
#[derive(Validate, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AwsAccessKeyCredentials {
    #[validate(regex(path = aws_access_key_id_regex_func(), message="AWS Access key ID is expected to be 20 chars alphanumeric string.\n"))]
    pub aws_access_key_id: String,
    #[validate(regex(path = aws_secret_access_key_regex_func(), message = "AWS Secret access key is expected to be 40 chars Base64-like string with uppercase, lowercase, digits, and +/= .\n"))]
    pub aws_secret_access_key: String,
}

impl std::fmt::Display for AwsAccessKeyCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "aws_access_key_id: {},",
            self.aws_access_key_id
        ))?;
        f.write_str("aws_secret_access_key: **********")
    }
}

impl std::fmt::Debug for AwsAccessKeyCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsAccessKeyCredentials")
            .field("aws_access_key_id", &self.aws_access_key_id)
            .field("aws_secret_access_key", &"**********")
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "credential_type", rename_all = "kebab-case")]
pub enum AwsCredentials {
    #[serde(rename = "access_key")]
    AccessKey(AwsAccessKeyCredentials),
    #[serde(rename = "token")]
    Token(String),
}

impl Validate for AwsCredentials {
    fn validate(&self) -> std::result::Result<(), ValidationErrors> {
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

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct S3Volume {
    #[validate(length(min = 1))]
    pub region: Option<String>,
    #[validate(length(min = 1), custom(function = "validate_bucket_name"))]
    pub bucket: Option<String>,
    #[validate(regex(path = s3_endpoint_regex_func(), message="Endpoint must start with https:// or http:// .\n"))]
    pub endpoint: Option<String>,
    #[validate(required, nested)]
    pub credentials: Option<AwsCredentials>,
}

impl S3Volume {
    #[must_use]
    pub fn get_s3_builder(&self) -> AmazonS3Builder {
        let mut s3_builder = AmazonS3Builder::new()
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);

        if let Some(region) = &self.region {
            s3_builder = s3_builder.with_region(region);
        }
        if let Some(bucket) = &self.bucket {
            s3_builder = s3_builder.with_bucket_name(bucket.clone());
        }
        if let Some(endpoint) = &self.endpoint {
            s3_builder = s3_builder.with_endpoint(endpoint);
            s3_builder = s3_builder.with_allow_http(endpoint.starts_with("http:"));
        }
        if let Some(credentials) = &self.credentials {
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
}

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct S3TablesVolume {
    #[validate(regex(path = s3_endpoint_regex_func(), message="Endpoint must start with https:// or http:// .\n"))]
    pub endpoint: Option<String>,
    #[validate(nested)]
    pub credentials: AwsCredentials,
    #[validate(regex(path = s3tables_arn_regex_func(), message="ARN must start with arn:aws:s3tables: .\n"))]
    pub arn: String,
}

impl S3TablesVolume {
    #[must_use]
    pub fn s3_builder(&self) -> AmazonS3Builder {
        let s3_volume = S3Volume {
            region: Some(self.region()),
            bucket: self.bucket(),
            // do not map `db_name` to the AmazonS3Builder
            endpoint: self.endpoint.clone(),
            credentials: Some(self.credentials.clone()),
        };
        s3_volume.get_s3_builder()
    }

    pub fn bucket(&self) -> Option<String> {
        // Get bucket name from S3Tables ARN
        // arn:aws:s3tables:us-east-1:111122223333:bucket/my-table-bucket
        self.arn.split(":bucket/").last().map(Into::into)
    }

    pub fn region(&self) -> String {
        self.arn
            .split(':')
            .nth(3)
            .map_or_else(|| "us-east-1".to_string(), Into::into)
    }

    pub fn account_id(&self) -> String {
        self.arn
            .split(':')
            .nth(4)
            .map(Into::into)
            .unwrap_or_default()
    }
}

fn validate_bucket_name(bucket_name: &str) -> std::result::Result<(), ValidationError> {
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

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct FileVolume {
    #[validate(length(min = 1))]
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
    fn validate(&self) -> std::result::Result<(), ValidationErrors> {
        match self {
            Self::S3(volume) => volume.validate(),
            Self::S3Tables(volume) => volume.validate(),
            Self::File(volume) => volume.validate(),
            Self::Memory => Ok(()),
        }
    }
}

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

    pub fn get_object_store(&self) -> Result<Arc<dyn ObjectStore>> {
        match &self.volume {
            VolumeType::S3(volume) => {
                let s3_builder = volume.get_s3_builder();
                s3_builder
                    .build()
                    .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                    .context(metastore_error::ObjectStoreSnafu)
            }
            VolumeType::S3Tables(volume) => {
                let s3_builder = volume.s3_builder();
                s3_builder
                    .build()
                    .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                    .context(metastore_error::ObjectStoreSnafu)
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
    pub fn prefix(&self) -> String {
        match &self.volume {
            VolumeType::S3(volume) => volume
                .bucket
                .as_ref()
                .map_or_else(|| "s3://".to_string(), |bucket| format!("s3://{bucket}")),
            VolumeType::S3Tables(volume) => volume
                .bucket()
                .map_or_else(|| "s3://".to_string(), |bucket| format!("s3://{bucket}")),
            VolumeType::File(volume) => format!("file://{}", volume.path),
            VolumeType::Memory => "memory://".to_string(),
        }
    }

    pub async fn validate_credentials(&self) -> Result<()> {
        let object_store = self.get_object_store()?;
        object_store
            .get(&Path::from(self.prefix()))
            .await
            .context(metastore_error::ObjectStoreSnafu)?;
        Ok(())
    }
}

/// Creates an `ObjectStore` from a URL string with optional endpoint.
/// This utility function handles creating object stores for different URL schemes.
///
/// # Arguments
/// * `url_str` - The URL string (e.g., "<s3://bucket/path>", "<file:///path>")
/// * `endpoint` - Optional custom endpoint for S3
///
/// # Returns
/// An `Arc<dyn ObjectStore>` that can be used for file operations
pub async fn create_object_store_from_url(
    url_str: &str,
    endpoint: Option<String>,
) -> Result<Arc<dyn ObjectStore + 'static>> {
    let url = url::Url::parse(url_str).context(metastore_error::UrlParseSnafu)?;

    match url.scheme() {
        "s3" => {
            let bucket = url.host_str().unwrap_or_default();

            let region = resolve_bucket_region(bucket, &ClientOptions::default())
                .await
                .context(metastore_error::ObjectStoreSnafu)?;

            let s3_volume = S3Volume {
                region: Some(region),
                bucket: Some(bucket.to_string()),
                endpoint,
                credentials: None,
            };

            let mut builder = s3_volume.get_s3_builder();
            builder = builder.with_skip_signature(true);

            let s3 = builder.build().context(metastore_error::ObjectStoreSnafu)?;
            Ok(Arc::new(s3))
        }
        "file" => {
            let local_fs = LocalFileSystem::new();
            Ok(Arc::new(local_fs))
        }
        _ => {
            let error = object_store::Error::Generic {
                store: "url_parser",
                source: format!("Unsupported URL scheme: {}", url.scheme()).into(),
            };
            Err(error).context(metastore_error::ObjectStoreSnafu)
        }
    }
}
