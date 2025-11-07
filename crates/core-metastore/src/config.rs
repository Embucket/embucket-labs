use crate::models::{Database, S3TablesVolume, Volume, VolumeIdent, VolumeType};
use serde::{Deserialize, Serialize};

/// Configuration for the basic metastore loaded from YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetastoreConfig {
    pub volumes: Vec<VolumeConfig>,
}

/// Volume configuration with optional databases
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeConfig {
    pub name: String,
    #[serde(flatten)]
    pub volume_type: S3TablesVolumeType,
    #[serde(default)]
    pub databases: Vec<DatabaseConfig>,
}

/// Only S3Tables volume type is supported in basic metastore
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum S3TablesVolumeType {
    S3Tables(S3TablesVolume),
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
}

impl MetastoreConfig {
    /// Load configuration from YAML file
    pub fn from_yaml_file(path: &str) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file: {e}"))?;
        Self::from_yaml_str(&content)
    }

    /// Load configuration from YAML string
    pub fn from_yaml_str(yaml: &str) -> Result<Self, String> {
        serde_yaml::from_str(yaml)
            .map_err(|e| format!("Failed to parse config YAML: {e}"))
    }
}

impl VolumeConfig {
    /// Convert to metastore Volume
    pub fn to_volume(&self) -> Volume {
        Volume {
            ident: self.name.clone(),
            volume: match &self.volume_type {
                S3TablesVolumeType::S3Tables(s3tables) => VolumeType::S3Tables(s3tables.clone()),
            },
        }
    }
}

impl DatabaseConfig {
    /// Convert to metastore Database
    pub fn to_database(&self, volume: &VolumeIdent) -> Database {
        Database {
            ident: self.name.clone(),
            volume: volume.clone(),
            properties: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let yaml = r#"
volumes:
  - name: my_volume
    type: s3_tables
    arn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
    endpoint: "https://s3tables.us-east-1.amazonaws.com"
    credentials: !AccessKey
      aws_access_key_id: "AKIAIOSFODNN7EXAMPLE"
      aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    databases:
      - name: my_db
      - name: another_db
"#;

        let config = MetastoreConfig::from_yaml_str(yaml).expect("Failed to parse config");
        assert_eq!(config.volumes.len(), 1);
        assert_eq!(config.volumes[0].name, "my_volume");
        assert_eq!(config.volumes[0].databases.len(), 2);
        assert_eq!(config.volumes[0].databases[0].name, "my_db");
    }
}
