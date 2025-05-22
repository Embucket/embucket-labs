use core_metastore::models::{
    AwsCredentials, FileVolume as MetastoreFileVolume, S3Volume as MetastoreS3Volume,
    Volume as MetastoreVolume, VolumeType as MetastoreVolumeType,
};
use core_metastore::{RwObject, S3TablesVolume as MetastoreS3TablesVolume};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct S3Volume {
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub skip_signature: Option<bool>,
    pub metadata_endpoint: Option<String>,
    pub credentials: Option<AwsCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct S3TablesVolume {
    pub region: String,
    pub bucket: Option<String>,
    pub endpoint: String,
    pub credentials: AwsCredentials,
    pub name: String,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct FileVolume {
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum VolumeType {
    S3(S3Volume),
    S3Tables(S3TablesVolume),
    File(FileVolume),
    Memory,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct VolumePayload {
    pub name: String,
    #[serde(flatten)]
    pub volume: VolumeType,
}

impl From<MetastoreVolume> for VolumePayload {
    fn from(volume: MetastoreVolume) -> Self {
        Self {
            name: volume.ident,
            volume: match volume.volume {
                MetastoreVolumeType::S3(volume) => VolumeType::S3(S3Volume {
                    region: volume.region,
                    bucket: volume.bucket,
                    endpoint: volume.endpoint,
                    skip_signature: volume.skip_signature,
                    metadata_endpoint: volume.metadata_endpoint,
                    credentials: volume.credentials,
                }),
                MetastoreVolumeType::S3Tables(volume) => VolumeType::S3Tables(S3TablesVolume {
                    region: volume.region,
                    bucket: volume.bucket,
                    endpoint: volume.endpoint,
                    credentials: volume.credentials,
                    name: volume.name,
                    arn: volume.arn,
                }),
                MetastoreVolumeType::File(file) => VolumeType::File(FileVolume { path: file.path }),
                MetastoreVolumeType::Memory => VolumeType::Memory,
            },
        }
    }
}

// TODO: Remove it when found why it can't locate .into() if only From trait implemeted
#[allow(clippy::from_over_into)]
impl Into<MetastoreVolume> for VolumePayload {
    fn into(self) -> MetastoreVolume {
        MetastoreVolume {
            ident: self.name,
            volume: match self.volume {
                VolumeType::S3(volume) => MetastoreVolumeType::S3(MetastoreS3Volume {
                    region: volume.region,
                    bucket: volume.bucket,
                    endpoint: volume.endpoint,
                    skip_signature: volume.skip_signature,
                    metadata_endpoint: volume.metadata_endpoint,
                    credentials: volume.credentials,
                }),
                VolumeType::S3Tables(volume) => {
                    MetastoreVolumeType::S3Tables(MetastoreS3TablesVolume {
                        region: volume.region,
                        bucket: volume.bucket,
                        endpoint: volume.endpoint,
                        credentials: volume.credentials,
                        name: volume.name,
                        arn: volume.arn,
                    })
                }
                VolumeType::File(volume) => {
                    MetastoreVolumeType::File(MetastoreFileVolume { path: volume.path })
                }
                VolumeType::Memory => MetastoreVolumeType::Memory,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeCreatePayload {
    #[serde(flatten)]
    pub data: VolumePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeUpdatePayload {
    #[serde(flatten)]
    pub data: VolumePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeCreateResponse {
    #[serde(flatten)]
    pub data: Volume,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeUpdateResponse {
    #[serde(flatten)]
    pub data: Volume,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeResponse {
    #[serde(flatten)]
    pub data: Volume,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub name: String,
    pub r#type: String,
    pub created_at: String,
    pub updated_at: String,
}

impl From<RwObject<MetastoreVolume>> for Volume {
    fn from(value: RwObject<MetastoreVolume>) -> Self {
        Self {
            name: value.data.ident,
            r#type: value.data.volume.to_string(),
            created_at: value.created_at.to_string(),
            updated_at: value.updated_at.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumesResponse {
    pub items: Vec<Volume>,
}
