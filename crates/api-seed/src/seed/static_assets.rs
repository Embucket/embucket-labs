use super::models::{VolumesRoot, Volume, VolumeGenerator};

const SEED_DATA: &str = include_str!("../../templates/seed.yaml");
const GEN_DATA: &str = include_str!("../../templates/gen.yaml");
const VOLUMES_ROOT_DATA: &str = include_str!("../../templates/volumes.yaml");

pub fn read_seed_template() -> Result<Volume, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<Volume>(SEED_DATA).map_err(|e| e.into())
}

pub fn read_gen_template() -> Result<VolumeGenerator, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<VolumeGenerator>(GEN_DATA).map_err(|e| e.into())
}

pub fn read_volumes_template() -> Result<VolumesRoot, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<VolumesRoot>(VOLUMES_ROOT_DATA).map_err(|e| e.into())
}

// pub fn generate_seed() -> Result<String, Box<dyn std::error::Error>> {
//     let template = read_seed_template()?;
//     // from template generate seed using
// }
