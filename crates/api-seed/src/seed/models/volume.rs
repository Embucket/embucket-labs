use super::{
    Generator, WithCount,
    database::{Database, DatabasesTemplateType},
};
use crate::seed::fake_provider::FakeProvider;
use api_structs::volumes::VolumeType;
use serde::{Deserialize, Serialize};

// This is different from metastore's equivalent
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Volume {
    pub volume_name: String,
    pub volume_type: VolumeType,
    pub databases: Vec<Database>,
}

#[allow(clippy::from_over_into)]
impl Into<api_structs::volumes::Volume> for Volume {
    fn into(self) -> api_structs::volumes::Volume {
        api_structs::volumes::Volume {
            name: self.volume_name,
            volume: self.volume_type,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum VolumesTemplateType {
    Volumes(Vec<Volume>),
    VolumesTemplate(WithCount<Volume, VolumeGenerator>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeGenerator {
    pub volume_name: Option<String>, // if None value will be generated
    pub volume_type: VolumeType,
    pub databases: DatabasesTemplateType,
}

impl Generator<Volume> for VolumeGenerator {
    fn generate(&self, index: usize) -> Volume {
        Volume {
            volume_name: self
                .volume_name
                .clone()
                .unwrap_or_else(FakeProvider::entity_name),
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
