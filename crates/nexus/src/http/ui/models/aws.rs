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
    pub fn new(
        aws_access_key_id: String,
        aws_secret_access_key: String,
    ) -> AwsAccessKeyCredential {
        AwsAccessKeyCredential {
            aws_access_key_id,
            aws_secret_access_key,
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
    pub fn new(
        role_arn: String,
        external_id: String,
    ) -> AwsRoleCredential {
        AwsRoleCredential {
            role_arn,
            external_id,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, ToSchema
)]
pub enum CloudProvider {
    #[serde(rename = "s3")]
    S3,
    #[serde(rename = "gcs")]
    Gcs,
    #[serde(rename = "azure")]
    Azure,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum Credentials {
    AwsAccessKeyCredential(AwsAccessKeyCredential),
    AwsRoleCredential(AwsRoleCredential),
}

impl Default for Credentials {
    fn default() -> Self {
        Credentials::AwsAccessKeyCredential(AwsAccessKeyCredential::default())
    }
}