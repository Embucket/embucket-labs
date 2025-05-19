use serde::{Deserialize, Serialize};
use super::{Generator, WithCount, database::{Database, DatabaseGenerator}};
use crate::seed::fake_provider::FakeProvider;
use api_structs::volumes::VolumeType;

use crate::seed::DatabaseSeed;

/*
Prototyping:
 
SeedRoot {
    volume_seeds: [
        volume_name,
        volume_type,
        database_seeds: [
            database_name,
            schema_seeds: [
                schema_name,
                table_seeds: [
                    Table { columns }
                ]
            ]
        ]
    ]
}

*/


#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VolumeSeedType {
    Volume(Volume),
    VolumeGenerator(VolumeGenerator),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VolumeSeed {
    #[serde(flatten)]
    pub volume: VolumeSeedType,
}

impl VolumeSeed {
    pub fn materialize(self) -> Volume {
        match self.volume {
            VolumeSeedType::VolumeGenerator(volume_generator) => volume_generator.generate(0),
            VolumeSeedType::Volume(volume) => volume,
        }
    }
}

// Use Volume struct defined here as it more expressive in yaml 
#[derive(Debug, Serialize, Deserialize)]
pub struct Volume {
    pub volume_name: String,
    pub volume_type: VolumeType,
    pub databases: Vec<Database>,
}

impl Into<api_structs::volumes::Volume> for Volume {
    fn into(self) -> api_structs::volumes::Volume {
        api_structs::volumes::Volume {
            name: self.volume_name,
            volume: self.volume_type,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VolumeGenerator {
    pub volume_name: Option<String>, // if None value will be generated
    pub volume_type: Option<VolumeType>,
    pub databases_gen: WithCount<Database, DatabaseGenerator>,
}

impl Generator<Volume> for VolumeGenerator {
    fn generate(&self, _index: usize) -> Volume {
        Volume {
            volume_name: self
                .volume_name
                .clone()
                .unwrap_or_else(|| FakeProvider::entity_name()),
            volume_type: self.volume_type.clone().unwrap_or(VolumeType::Memory),
            databases: self.databases_gen.generate(),
        }
    }
}
