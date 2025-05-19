use crate::seed::models::{Generator, VolumeSeed, VolumeSeedType, Volume};
use crate::seed::rng::{SEED_FOR_RANDOMIZER, init_rng};
use crate::seed::{
    read_gen_template, read_seed_root_template, read_seed_template, read_super_template, SeedRoot,
};
use api_structs::volumes::VolumeType;
use api_structs::volumes::FileVolume;

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
    let seed_root = SeedRoot {
        volumes: vec![VolumeSeed {
            volume: VolumeSeedType::Volume(Volume {
                volume_name: "foo".to_string(),
                volume_type: VolumeType::File(FileVolume {
                    path: "bar".to_string(),
                }),
                databases: vec![
                    Database {
                        name: "baz".to_string(),
                        schemas: vec![
                            Schema {
                                name: "qux".to_string(),
                                tables: vec![
                                    Table {
                                        name: "quux".to_string(),
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }),
        }],
    };

    eprintln!(
        "programmatically created #1: \n{}",
        serde_yaml::to_string(&super_volume).unwrap()
    );

    let super_volume = VolumeSeed {
        volume: VolumeSeedType::VolumeGenerator(
            read_gen_template().expect("Failed to read seed gen"),
        ),
    };
    eprintln!(
        "programmatically created #2: \n{}",
        serde_yaml::to_string(&super_volume).unwrap()
    );

    let seed_gen = read_super_template().expect("Failed to read seed gen");
    eprintln!("loaded from file: {:#?}", seed_gen);
    let data = seed_gen.materialize();
    eprintln!("materialized: {:#?}", data);
    assert!(false);
}

#[test]
fn test_seed_root() {
    init_rng(SEED_FOR_RANDOMIZER);
    let seed_root = read_seed_root_template().expect("Failed to read seed root");
    eprintln!("{:#?}", seed_root);
}
