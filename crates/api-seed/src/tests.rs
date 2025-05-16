use super::Volume;

use crate::{
    SEED_FOR_RANDOMIZER, read_super_template, read_seed_template, read_gen_template, init_rng,
};
use crate::models::{Generator, SuperVolume, SuperVolumeType, VolumeGenerator,};

#[test]
fn test_seed() {
    init_rng(SEED_FOR_RANDOMIZER);
    let seed = read_seed_template().expect("Failed to read seed");
    eprintln!("{:#?}", seed);
}

#[test]
fn test_gen() {
    init_rng(SEED_FOR_RANDOMIZER);
    let seed_gen = read_gen_template().expect("Failed to read seed gen");
    eprintln!("{:#?}", seed_gen);
    let data = seed_gen.generate(0);
    eprintln!("{:#?}", data);
}

#[test]
fn test_super_gen() {
    init_rng(SEED_FOR_RANDOMIZER);
    let super_volume = SuperVolume { 
        volume: SuperVolumeType::Volume(Volume {
            volume_name: "foo".to_string(),
            databases: vec![],
        })
    };
    eprintln!("programmatically created #1: \n{}", serde_yaml::to_string(&super_volume).unwrap());

    let super_volume = SuperVolume { 
        volume: SuperVolumeType::VolumeGenerator(
            read_gen_template().expect("Failed to read seed gen")
        )
    };
    eprintln!("programmatically created #2: \n{}", serde_yaml::to_string(&super_volume).unwrap());

    let seed_gen = read_super_template().expect("Failed to read seed gen");
    eprintln!("loaded from file: {:#?}", seed_gen);
    let data = seed_gen.materialize();
    eprintln!("materialized: {:#?}", data);
    assert!(false);
}
