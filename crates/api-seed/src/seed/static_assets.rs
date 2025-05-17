
use rand::{SeedableRng, rngs::StdRng};
use fake::{Dummy, Fake, };
use std::cell::RefCell;
use super::models::{SeedRoot, SuperVolume, VolumeGenerator, Volume, Database};


const SEED_DATA: &str = include_str!("../../templates/seed.yaml");
const GEN_DATA: &str = include_str!("../../templates/gen.yaml");
const SUPER_GEN_DATA: &str = include_str!("../../templates/super.yaml");
const SEED_ROOT_DATA: &str = include_str!("../../templates/seed_root.yaml");




pub fn read_seed_template() -> Result<Volume, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<Volume>(SEED_DATA).map_err(|e| e.into())
}

pub fn read_gen_template() -> Result<VolumeGenerator, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<VolumeGenerator>(GEN_DATA).map_err(|e| e.into())
}

pub fn read_super_template() -> Result<SuperVolume, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<SuperVolume>(SUPER_GEN_DATA).map_err(|e| e.into())
}

pub fn read_seed_root_template() -> Result<SeedRoot, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<SeedRoot>(SEED_ROOT_DATA).map_err(|e| e.into())
}

// pub fn generate_seed() -> Result<String, Box<dyn std::error::Error>> {
//     let template = read_seed_template()?;
//     // from template generate seed using 
// }
