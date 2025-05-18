use crate::default_limit;
use core_metastore::S3TablesVolume as MetastoreS3TablesVolume;
use core_metastore::models::{
    AwsCredentials, FileVolume as MetastoreFileVolume, S3Volume as MetastoreS3Volume,
    Volume as MetastoreVolume, VolumeType as MetastoreVolumeType,
};
use api_structs::volumes::Volume;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};


#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeUpdatePayload {
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
    #[serde(default = "default_limit")]
    pub limit: Option<u16>,
    pub search: Option<String>,
}
