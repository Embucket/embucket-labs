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

use control_plane::models;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AwsAccessKeyCredential {
    #[validate(length(min = 1))]
    pub aws_access_key_id: String,
    #[validate(length(min = 1))]
    pub aws_secret_access_key: String,
}

impl AwsAccessKeyCredential {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(aws_access_key_id: String, aws_secret_access_key: String) -> Self {
        Self {
            aws_access_key_id,
            aws_secret_access_key,
        }
    }
}

impl Serialize for AwsAccessKeyCredential {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AwsAccessKeyCredential", 2)?;
        state.serialize_field("awsAccessKeyId", &self.aws_access_key_id)?;
        state.serialize_field("awsSecretAccessKey", &"********")?;
        state.end()
    }
}

impl From<AwsAccessKeyCredential> for models::AwsAccessKeyCredential {
    fn from(credential: AwsAccessKeyCredential) -> Self {
        Self {
            aws_access_key_id: credential.aws_access_key_id,
            aws_secret_access_key: credential.aws_secret_access_key,
        }
    }
}
impl From<models::AwsAccessKeyCredential> for AwsAccessKeyCredential {
    fn from(credential: models::AwsAccessKeyCredential) -> Self {
        Self {
            aws_access_key_id: credential.aws_access_key_id,
            aws_secret_access_key: credential.aws_secret_access_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AwsRoleCredential {
    #[validate(length(min = 1))]
    pub role_arn: String,
    #[validate(length(min = 1))]
    pub external_id: String,
}

impl AwsRoleCredential {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(role_arn: String, external_id: String) -> Self {
        Self {
            role_arn,
            external_id,
        }
    }
}

impl From<AwsRoleCredential> for models::AwsRoleCredential {
    fn from(credential: AwsRoleCredential) -> Self {
        Self {
            role_arn: credential.role_arn,
            external_id: credential.external_id,
        }
    }
}
impl From<models::AwsRoleCredential> for AwsRoleCredential {
    fn from(credential: models::AwsRoleCredential) -> Self {
        Self {
            role_arn: credential.role_arn,
            external_id: credential.external_id,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Hash,
    Default,
    ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum CloudProvider {
    #[serde(rename = "aws")]
    #[default]
    AWS,
    #[serde(rename = "gcs")]
    GCS,
    #[serde(rename = "azure")]
    AZURE,
    #[serde(rename = "fs")]
    FS,
}

impl From<models::CloudProvider> for CloudProvider {
    fn from(provider: models::CloudProvider) -> Self {
        match provider {
            models::CloudProvider::AWS => Self::AWS,
            models::CloudProvider::GCS => Self::GCS,
            models::CloudProvider::AZURE => Self::AZURE,
            models::CloudProvider::FS => Self::FS,
        }
    }
}

impl From<CloudProvider> for models::CloudProvider {
    fn from(provider: CloudProvider) -> Self {
        match provider {
            CloudProvider::AWS => Self::AWS,
            CloudProvider::GCS => Self::GCS,
            CloudProvider::AZURE => Self::AZURE,
            CloudProvider::FS => Self::FS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all(serialize = "camelCase", deserialize = "camelCase"))]
#[serde(untagged)]
pub enum Credentials {
    AccessKey(AwsAccessKeyCredential),
    Role(AwsRoleCredential),
}

impl Default for Credentials {
    fn default() -> Self {
        Self::AccessKey(AwsAccessKeyCredential::default())
    }
}
impl From<models::Credentials> for Credentials {
    fn from(credentials: models::Credentials) -> Self {
        match credentials {
            models::Credentials::AccessKey(creds) => Self::AccessKey(AwsAccessKeyCredential::new(
                creds.aws_access_key_id,
                creds.aws_secret_access_key,
            )),
            models::Credentials::Role(creds) => {
                Self::Role(AwsRoleCredential::new(creds.role_arn, creds.external_id))
            }
        }
    }
}

impl From<Credentials> for models::Credentials {
    fn from(credentials: Credentials) -> Self {
        match credentials {
            Credentials::AccessKey(creds) => Self::AccessKey(models::AwsAccessKeyCredential {
                aws_access_key_id: creds.aws_access_key_id,
                aws_secret_access_key: creds.aws_secret_access_key,
            }),
            Credentials::Role(creds) => Self::Role(models::AwsRoleCredential {
                role_arn: creds.role_arn,
                external_id: creds.external_id,
            }),
        }
    }
}
