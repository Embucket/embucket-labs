
#[cfg(test)]
pub mod tests;
pub mod models;


use rand::{SeedableRng, rngs::StdRng};
use fake::{Dummy, Fake, faker::internet::en::SafeEmail};
use std::cell::RefCell;
use crate::models::{SuperVolume, VolumeGenerator, Volume, Database};









pub const SEED_FOR_RANDOMIZER: u64 = 1024;
const SEED_DATA: &str = include_str!("../templates/seed.yaml");
const GEN_DATA: &str = include_str!("../templates/gen.yaml");
const SUPER_GEN_DATA: &str = include_str!("../templates/super.yaml");


// fn anonymize(record: &Record) -> Record {
//     Record {
//         name: Name().fake(),
//         email: SafeEmail().fake(),
//         ip: "0.0.0.0".into(), // static anonymization
//     }
// }

thread_local! {
    static RNG: RefCell<StdRng> = RefCell::new(StdRng::seed_from_u64(SEED_FOR_RANDOMIZER));
}

/// Faker that uses a thread-local seeded RNG
pub trait GlobalFaker: Dummy<StdRng> {
    fn fake_global() -> Self where Self: Sized {
        RNG.with(|rng| Self::dummy(&mut *rng.borrow_mut()))
    }
}

pub fn init_rng(seed: u64) {
    RNG.with(|r| *r.borrow_mut() = StdRng::seed_from_u64(seed));
}

// Blanket impl for all `Dummy<StdRng>` types
impl<T: Dummy<StdRng>> GlobalFaker for T {}


pub fn read_seed_template() -> Result<Volume, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<Volume>(SEED_DATA).map_err(|e| e.into())
}

pub fn read_gen_template() -> Result<VolumeGenerator, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<VolumeGenerator>(GEN_DATA).map_err(|e| e.into())
}

pub fn read_super_template() -> Result<SuperVolume, Box<dyn std::error::Error>> {
    serde_yaml::from_str::<SuperVolume>(SUPER_GEN_DATA).map_err(|e| e.into())
}

// pub fn generate_seed() -> Result<String, Box<dyn std::error::Error>> {
//     let template = read_seed_template()?;
//     // from template generate seed using 
// }
