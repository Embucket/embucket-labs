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

use icebucket_metastore::models::{
    AwsCredentials, IceBucketFileVolume, IceBucketS3Volume, IceBucketVolume, IceBucketVolumeType,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

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
pub struct FileVolume {
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum VolumeType {
    S3(S3Volume),
    File(FileVolume),
    Memory,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct Volume {
    pub name: String,
    #[serde(flatten)]
    pub volume: VolumeType,
}

impl From<IceBucketVolume> for Volume {
    fn from(volume: IceBucketVolume) -> Self {
        Self {
            name: volume.ident,
            volume: match volume.volume {
                IceBucketVolumeType::S3(volume) => VolumeType::S3(S3Volume {
                    region: volume.region,
                    bucket: volume.bucket,
                    endpoint: volume.endpoint,
                    skip_signature: volume.skip_signature,
                    metadata_endpoint: volume.metadata_endpoint,
                    credentials: volume.credentials,
                }),
                IceBucketVolumeType::File(file) => VolumeType::File(FileVolume { path: file.path }),
                IceBucketVolumeType::Memory => VolumeType::Memory,
            },
        }
    }
}

// TODO: Remove it when found why it can't locate .into() if only From trait implemeted
#[allow(clippy::from_over_into)]
impl Into<IceBucketVolume> for Volume {
    fn into(self) -> IceBucketVolume {
        IceBucketVolume {
            ident: self.name,
            volume: match self.volume {
                VolumeType::S3(volume) => IceBucketVolumeType::S3(IceBucketS3Volume {
                    region: volume.region,
                    bucket: volume.bucket,
                    endpoint: volume.endpoint,
                    skip_signature: volume.skip_signature,
                    metadata_endpoint: volume.metadata_endpoint,
                    credentials: volume.credentials,
                }),
                VolumeType::File(volume) => {
                    IceBucketVolumeType::File(IceBucketFileVolume { path: volume.path })
                }
                VolumeType::Memory => IceBucketVolumeType::Memory,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeCreatePayload {
    #[serde(flatten)]
    pub data: Volume,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeUpdatePayload {
    #[serde(flatten)]
    pub data: Volume,
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
pub struct VolumesResponse {
    pub items: Vec<Volume>,
    pub current_cursor: Option<String>,
    pub next_cursor: String,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct VolumesParameters {
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}
