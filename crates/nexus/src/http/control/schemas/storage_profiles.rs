use chrono::NaiveDateTime;
use control_plane::models;
use serde::{Deserialize, Serialize};
use std::option::Option;
use utoipa::ToSchema;
use uuid::Uuid;

// Define the cloud provider enum
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum CloudProvider {
    Aws,
    Azure,
    Gcp,
}

impl From<CloudProvider> for models::CloudProvider {
    fn from(provider: CloudProvider) -> Self {
        match provider {
            CloudProvider::Aws => models::CloudProvider::AWS,
            CloudProvider::Azure => models::CloudProvider::AZURE,
            CloudProvider::Gcp => models::CloudProvider::GCS,
        }
    }
}
impl From<models::CloudProvider> for CloudProvider {
    fn from(provider: models::CloudProvider) -> Self {
        match provider {
            models::CloudProvider::AWS => CloudProvider::Aws,
            models::CloudProvider::AZURE => CloudProvider::Azure,
            models::CloudProvider::GCS => CloudProvider::Gcp,
        }
    }
}

// AWS Access Key Credentials
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct AwsAccessKeyCredential {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
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

// AWS Role Credentials
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct AwsRoleCredential {
    pub role_arn: String,
    pub external_id: String,
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

// Enum to represent either Access Key or Role Credentials
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(tag = "credential_type")] // Enables tagged union based on credential type
#[serde(rename_all = "kebab-case")]
pub enum Credentials {
    #[serde(rename = "access_key")]
    AccessKey(AwsAccessKeyCredential),

    #[serde(rename = "role")]
    Role(AwsRoleCredential),
}

impl From<Credentials> for models::Credentials {
    fn from(credential: Credentials) -> Self {
        match credential {
            Credentials::AccessKey(aws_credential) => {
                models::Credentials::AccessKey(aws_credential.into())
            }
            Credentials::Role(role_credential) => models::Credentials::Role(role_credential.into()),
        }
    }
}
impl From<models::Credentials> for Credentials {
    fn from(credential: models::Credentials) -> Self {
        match credential {
            models::Credentials::AccessKey(aws_credential) => {
                Credentials::AccessKey(aws_credential.into())
            }
            models::Credentials::Role(role_credential) => Credentials::Role(role_credential.into()),
        }
    }
}

// Request struct for creating a storage profile
#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateStorageProfilePayload {
    #[serde(rename = "type")]
    pub provider_type: CloudProvider,
    pub region: String,
    pub bucket: String,
    pub credentials: Credentials,
    pub sts_role_arn: Option<String>,
    pub endpoint: Option<String>,
}

impl From<CreateStorageProfilePayload> for models::StorageProfileCreateRequest {
    fn from(payload: CreateStorageProfilePayload) -> Self {
        models::StorageProfileCreateRequest {
            cloud_provider: payload.provider_type.into(),
            region: payload.region,
            bucket: payload.bucket,
            credentials: payload.credentials.into(),
            sts_role_arn: payload.sts_role_arn,
            endpoint: payload.endpoint,
        }
    }
}

// Response struct for returning a storage profile
#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct StorageProfile {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub r#type: CloudProvider,
    pub region: String,
    pub bucket: String,
    pub credentials: Credentials,
    pub sts_role_arn: Option<String>,
    pub endpoint: Option<String>,

    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl From<models::StorageProfile> for StorageProfile {
    fn from(profile: models::StorageProfile) -> Self {
        StorageProfile {
            id: profile.id,
            r#type: profile.cloud_provider.into(),
            region: profile.region,
            bucket: profile.bucket,
            credentials: profile.credentials.into(),
            sts_role_arn: profile.sts_role_arn,
            endpoint: profile.endpoint,
            created_at: profile.created_at,
            updated_at: profile.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn test_deserialize_create_storage_profile_payload() {
        let payload = r#"
            {
                "type": "aws",
                "region": "us-west-2",
                "bucket": "my-bucket",
                "credentials": {
                    "credential_type": "access_key",
                    "aws_access_key_id": "my-access-key",
                    "aws_secret_access_key": "my-secret-access-key"
                }
            }
        "#;

        let result: CreateStorageProfilePayload = serde_json::from_str(payload).unwrap();
        assert_eq!(result.region, "us-west-2");
        assert_eq!(result.bucket, "my-bucket");
        assert_eq!(result.provider_type, CloudProvider::Aws);
        assert_eq!(
            result.credentials,
            Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "my-access-key".to_string(),
                aws_secret_access_key: "my-secret-access-key".to_string(),
            })
        );
    }

    #[test]
    fn test_serialize_create_storage_profile_payload() {
        let payload = CreateStorageProfilePayload {
            provider_type: CloudProvider::Aws,
            region: "us-west-2".to_string(),
            bucket: "my-bucket".to_string(),
            credentials: Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "my-access-key".to_string(),
                aws_secret_access_key: "my-secret-access-key".to_string(),
            }),
            sts_role_arn: None,
            endpoint: None,
        };

        let result = serde_json::to_string(&payload).unwrap();
        println!("{result}");
        let expected = r#"{"type":"aws","region":"us-west-2","bucket":"my-bucket","credentials":{"credential_type":"access_key","aws_access_key_id":"my-access-key","aws_secret_access_key":"my-secret-access-key"},"sts_role_arn":null,"endpoint":null}"#;
        assert_eq!(result, expected);
    }
}
