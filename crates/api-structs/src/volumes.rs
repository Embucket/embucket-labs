use super::with_derives;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use utoipa::ToSchema;

use core_metastore::S3TablesVolume as MetastoreS3TablesVolume;
use core_metastore::models::{
    AwsCredentials, FileVolume as MetastoreFileVolume, S3Volume as MetastoreS3Volume,
    Volume as MetastoreVolume, VolumeType as MetastoreVolumeType,
};

with_derives! {
    #[derive(Debug, Clone)]
    pub struct S3Volume {
        pub region: Option<String>,
        pub bucket: Option<String>,
        pub endpoint: Option<String>,
        pub skip_signature: Option<bool>,
        pub metadata_endpoint: Option<String>,
        pub credentials: Option<AwsCredentials>,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct S3TablesVolume {
        pub region: String,
        pub bucket: Option<String>,
        pub endpoint: String,
        pub credentials: AwsCredentials,
        pub name: String,
        pub arn: String,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct FileVolume {
        pub path: String,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub enum VolumeType {
        S3(S3Volume),
        S3Tables(S3TablesVolume),
        File(FileVolume),
        Memory,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct Volume {
        pub name: String,
        #[serde(flatten)]
        pub volume: VolumeType,
    }
}

impl From<MetastoreVolume> for Volume {
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

#[allow(clippy::from_over_into)]
impl Into<MetastoreVolume> for Volume {
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

with_derives! {
    #[derive(Debug, Clone)]
    pub struct VolumeCreatePayload {
        #[serde(flatten)]
        pub data: Volume,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct VolumeCreateResponse {
        #[serde(flatten)]
        pub data: Volume,
    }
}
