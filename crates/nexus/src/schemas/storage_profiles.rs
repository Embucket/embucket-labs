use serde::{Deserialize, Serialize};
use chrono::NaiveDateTime;
use uuid::Uuid;
use std::option::Option;

// Define the cloud provider enum
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CloudProvider {
    Aws,
    Azure,
    Gcp,
}

// AWS Access Key Credentials
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AwsAccessKeyCredential {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
}

// AWS Role Credentials
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AwsRoleCredential {
    pub role_arn: String,
    pub external_id: String,
}

// Enum to represent either Access Key or Role Credentials
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "credential_type")]  // Enables tagged union based on credential type
pub enum Credentials {
    #[serde(rename = "access_key")]
    AccessKey(AwsAccessKeyCredential),

    #[serde(rename = "role")]
    Role(AwsRoleCredential),
}

// Request struct for creating a storage profile
#[derive(Serialize, Deserialize, Debug)]
pub struct CreateStorageProfilePayload {
    #[serde(rename = "type")]
    pub provider_type: CloudProvider,
    pub region: String,
    pub bucket: String,
    pub credentials: Credentials,
    pub sts_role_arn: Option<String>,
    pub endpoint: Option<String>,
}

// Response struct for returning a storage profile
#[derive(Serialize, Deserialize, Debug)]
pub struct StorageProfile {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub provider_type: CloudProvider,
    pub region: String,
    pub bucket: String,
    pub credentials: Credentials,
    pub sts_role_arn: Option<String>,
    pub endpoint: Option<String>,

    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
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
        assert_eq!(result.credentials, Credentials::AccessKey(AwsAccessKeyCredential {
            aws_access_key_id: "my-access-key".to_string(),
            aws_secret_access_key: "my-secret-access-key".to_string(),
        }));
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