use crate::SeedVariant;
use crate::seed::models::VolumesRoot;

const MINIMAL_SEED_DATA: &str = include_str!("../templates/minimal_seed.yaml");
const TYPICAL_SEED_DATA: &str = include_str!("../templates/typical_seed.yaml");

pub fn read_seed_template(
    seed_variant: SeedVariant,
) -> Result<VolumesRoot, Box<dyn std::error::Error>> {
    match seed_variant {
        SeedVariant::Minimal => {
            serde_yaml::from_str::<VolumesRoot>(MINIMAL_SEED_DATA).map_err(std::convert::Into::into)
        }
        SeedVariant::Typical => {
            serde_yaml::from_str::<VolumesRoot>(TYPICAL_SEED_DATA).map_err(std::convert::Into::into)
        }
        _ => serde_yaml::from_str::<VolumesRoot>(TYPICAL_SEED_DATA).map_err(std::convert::Into::into),
    }
}

// pub fn generate_seed() -> Result<String, Box<dyn std::error::Error>> {
//     let template = read_seed_template()?;
//     // from template generate seed using
// }
