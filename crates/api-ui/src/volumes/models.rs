use core_metastore::models::{
    AwsAccessKeyCredentials as MetastoreAwsAccessKeyCredentials,
    AwsCredentials as MetastoreAwsCredentials, FileVolume as MetastoreFileVolume,
    S3Volume as MetastoreS3Volume, Volume as MetastoreVolume, VolumeType as MetastoreVolumeType,
};
use core_metastore::{RwObject, S3TablesVolume as MetastoreS3TablesVolume, error as metastore_err};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AwsAccessKeyCredentials {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum AwsCredentials {
    AccessKey(AwsAccessKeyCredentials),
    Token(String),
}

#[allow(clippy::from_over_into)]
impl Into<MetastoreAwsCredentials> for AwsCredentials {
    fn into(self) -> MetastoreAwsCredentials {
        match self {
            Self::AccessKey(access_key) => {
                MetastoreAwsCredentials::AccessKey(MetastoreAwsAccessKeyCredentials {
                    aws_access_key_id: access_key.aws_access_key_id,
                    aws_secret_access_key: access_key.aws_secret_access_key,
                })
            }
            Self::Token(token) => MetastoreAwsCredentials::Token(token),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct S3Volume {
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub credentials: Option<AwsCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct S3TablesVolume {
    pub endpoint: Option<String>,
    pub credentials: AwsCredentials,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct FileVolume {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum VolumeType {
    S3(S3Volume),
    S3Tables(S3TablesVolume),
    File(FileVolume),
    Memory,
}

#[allow(clippy::from_over_into)]
impl Into<MetastoreVolumeType> for VolumeType {
    fn into(self) -> MetastoreVolumeType {
        match self {
            Self::S3(volume) => MetastoreVolumeType::S3(MetastoreS3Volume {
                region: volume.region,
                bucket: volume.bucket,
                endpoint: volume.endpoint,
                credentials: volume.credentials.map(AwsCredentials::into),
            }),
            Self::S3Tables(volume) => MetastoreVolumeType::S3Tables(MetastoreS3TablesVolume {
                endpoint: volume.endpoint,
                credentials: volume.credentials.into(),
                arn: volume.arn,
            }),
            Self::File(volume) => {
                MetastoreVolumeType::File(MetastoreFileVolume { path: volume.path })
            }
            Self::Memory => MetastoreVolumeType::Memory,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeCreatePayload {
    pub name: String,
    #[serde(flatten)]
    pub volume: VolumeType,
}

// #[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
// #[serde(rename_all = "camelCase")]
// pub struct VolumeUpdatePayload {
//     pub name: Option<String>,
// }

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeCreateResponse(pub Volume);

// #[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
// #[serde(rename_all = "camelCase")]
// pub struct VolumeUpdateResponse(pub Volume);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeResponse(pub Volume);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub id: i64,
    pub name: String,
    pub r#type: String,
    pub created_at: String,
    pub updated_at: String,
}

impl TryFrom<RwObject<MetastoreVolume>> for Volume {
    type Error = metastore_err::Error;
    fn try_from(value: RwObject<MetastoreVolume>) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            id: value.id().context(metastore_err::NoIdSnafu)?,
            name: value.data.ident,
            r#type: value.data.volume.to_string(),
            created_at: value.created_at.to_string(),
            updated_at: value.updated_at.to_string(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumesResponse {
    pub items: Vec<Volume>,
}
