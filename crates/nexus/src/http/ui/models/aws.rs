use control_plane::models;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, Validate, ToSchema)]
pub struct AwsAccessKeyCredential {
    #[validate(length(min = 1))]
    pub aws_access_key_id: String,
    #[validate(length(min = 1))]
    pub aws_secret_access_key: String,
}

impl AwsAccessKeyCredential {
    #[allow(clippy::new_without_default)]
    pub fn new(aws_access_key_id: String, aws_secret_access_key: String) -> AwsAccessKeyCredential {
        AwsAccessKeyCredential {
            aws_access_key_id,
            aws_secret_access_key,
        }
    }
}

impl From<AwsAccessKeyCredential> for models::AwsAccessKeyCredential {
    fn from(credential: AwsAccessKeyCredential) -> Self {
        models::AwsAccessKeyCredential {
            aws_access_key_id: credential.aws_access_key_id,
            aws_secret_access_key: credential.aws_secret_access_key,
        }
    }
}
impl From<models::AwsAccessKeyCredential> for AwsAccessKeyCredential {
    fn from(credential: models::AwsAccessKeyCredential) -> Self {
        AwsAccessKeyCredential {
            aws_access_key_id: credential.aws_access_key_id,
            aws_secret_access_key: credential.aws_secret_access_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct AwsRoleCredential {
    #[validate(length(min = 1))]
    pub role_arn: String,
    #[validate(length(min = 1))]
    pub external_id: String,
}

impl AwsRoleCredential {
    #[allow(clippy::new_without_default)]
    pub fn new(role_arn: String, external_id: String) -> AwsRoleCredential {
        AwsRoleCredential {
            role_arn,
            external_id,
        }
    }
}

impl From<AwsRoleCredential> for models::AwsRoleCredential {
    fn from(credential: AwsRoleCredential) -> Self {
        models::AwsRoleCredential {
            role_arn: credential.role_arn,
            external_id: credential.external_id,
        }
    }
}
impl From<models::AwsRoleCredential> for AwsRoleCredential {
    fn from(credential: models::AwsRoleCredential) -> Self {
        AwsRoleCredential {
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
pub enum CloudProvider {
    #[serde(rename = "aws")]
    #[default]
    AWS,
    #[serde(rename = "gcs")]
    GCS,
    #[serde(rename = "azure")]
    AZURE,
}

impl From<models::CloudProvider> for CloudProvider {
    fn from(provider: models::CloudProvider) -> Self {
        match provider {
            models::CloudProvider::AWS => CloudProvider::AWS,
            models::CloudProvider::GCS => CloudProvider::GCS,
            models::CloudProvider::AZURE => CloudProvider::AZURE,
        }
    }
}

impl From<CloudProvider> for models::CloudProvider {
    fn from(provider: CloudProvider) -> Self {
        match provider {
            CloudProvider::AWS => models::CloudProvider::AWS,
            CloudProvider::GCS => models::CloudProvider::GCS,
            CloudProvider::AZURE => models::CloudProvider::AZURE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum Credentials {
    #[serde(rename = "access_key")]
    AccessKey(AwsAccessKeyCredential),
    #[serde(rename = "role")]
    Role(AwsRoleCredential),
}

impl Default for Credentials {
    fn default() -> Self {
        Credentials::AccessKey(AwsAccessKeyCredential::default())
    }
}
impl From<models::Credentials> for Credentials {
    fn from(credentials: models::Credentials) -> Self {
        match credentials {
            models::Credentials::AccessKey(creds) => Credentials::AccessKey(
                AwsAccessKeyCredential::new(creds.aws_access_key_id, creds.aws_secret_access_key),
            ),
            models::Credentials::Role(creds) => {
                Credentials::Role(AwsRoleCredential::new(creds.role_arn, creds.external_id))
            }
        }
    }
}

impl From<Credentials> for models::Credentials {
    fn from(credentials: Credentials) -> Self {
        match credentials {
            Credentials::AccessKey(creds) => models::Credentials::AccessKey(
                models::AwsAccessKeyCredential {
                    aws_access_key_id: creds.aws_access_key_id,
                    aws_secret_access_key: creds.aws_secret_access_key,
                },
            ),
            Credentials::Role(creds) => {
                models::Credentials::Role(models::AwsRoleCredential {
                    role_arn: creds.role_arn,
                    external_id: creds.external_id,
                })
            }
        }
    }
}
