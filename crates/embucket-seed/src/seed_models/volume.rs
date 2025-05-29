use serde::{Deserialize, Serialize};

use super::database::{Database, DatabasesTemplateType};
use crate::external_models::{VolumeCreatePayload, VolumeType};
use crate::seed_generator::{Generator, fake_provider::FakeProvider};

// This is different from metastore's equivalent
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Volume {
    pub volume_name: String,
    #[serde(flatten)]
    pub volume_type: VolumeType,
    pub databases: Vec<Database>,
}

#[allow(clippy::from_over_into)]
impl Into<VolumeCreatePayload> for Volume {
    fn into(self) -> VolumeCreatePayload {
        VolumeCreatePayload {
            name: self.volume_name,
            volume: self.volume_type,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeGenerator {
    pub volume_name: Option<String>, // if None value will be generated
    #[serde(flatten)]
    pub volume_type: VolumeType,
    pub databases: DatabasesTemplateType,
}

impl Generator<Volume> for VolumeGenerator {
    fn generate(&self, index: usize) -> Volume {
        Volume {
            volume_name: self
                .volume_name
                .clone()
                .unwrap_or_else(|| FakeProvider::entity_name(index)),
            volume_type: self.volume_type.clone(),
            databases: match &self.databases {
                DatabasesTemplateType::DatabasesTemplate(db_template) => {
                    // handle WithCount template
                    db_template.vec_with_count(index)
                }
                DatabasesTemplateType::Databases(dbs) => dbs.clone(),
            },
        }
    }
}
