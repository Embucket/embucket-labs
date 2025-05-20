use crate::seed::rng::{SEED_FOR_RANDOMIZER, init_rng};
use crate::seed::{
    ColumnGenerator, DatabaseGenerator, SchemaGenerator, TableGenerator, VolumeGenerator,
    VolumesRoot,
};
use crate::seed::{
    ColumnsTemplateType, DatabasesTemplateType, SchemasTemplateType, TablesTemplateType,
};
use crate::static_assets::read_seed_template;
use crate::{SeedApi, SeedDatabase, SeedVariant};
use api_structs::volumes::{FileVolume, VolumeType};

use crate::seed::{Column, ColumnType, Database, Schema, Table, WithCount};
use api_ui::test_server::run_test_server_with_demo_auth;

#[tokio::test]
async fn test_seed_client() {
    let addr =
        run_test_server_with_demo_auth("secret".to_string(), "user1".to_string(), "pass1".to_string())
            .await;

    init_rng(SEED_FOR_RANDOMIZER);

    let mut seed_db = SeedDatabase::new(addr);
    seed_db
        .try_load_seed_template(SeedVariant::Typical)
        .expect("Failed to load seed template");
    seed_db
        .login("user1", "pass1")
        .await
        .expect("Failed to login");
    seed_db.seed_all().await.expect("Failed to seed database");
}

#[test]
fn test_minimal_seed() {
    init_rng(SEED_FOR_RANDOMIZER);
    // Create root to serialize it to yaml and add to a template file
    let seed_root = VolumesRoot {
        volumes: vec![
            // expected static data, no enums expected
            VolumeGenerator {
                volume_name: Some("minimal".to_string()),
                volume_type: VolumeType::Memory,
                databases: DatabasesTemplateType::Databases(vec![Database {
                    database_name: "db1".to_string(),
                    schemas: vec![Schema {
                        schema_name: "schema1".to_string(),
                        tables: vec![Table {
                            name: "table1".to_string(),
                            columns: vec![Column {
                                col_name: "col1".to_string(),
                                col_type: ColumnType::String,
                            }],
                        }],
                    }],
                }]),
            },
        ],
    };

    // Save output of ^^ this to minimal_seed.yaml when changing code ^^

    eprintln!(
        "programmatically created minimal seed template: \n{}",
        serde_yaml::to_string(&seed_root).expect("Failed to serialize seed template")
    );

    let seed_template =
        read_seed_template(SeedVariant::Minimal).expect("Failed to read seed template");
    assert_eq!(seed_root, seed_template);

    let data = seed_template.generate();
    eprintln!(
        "generated seed data: \n{}",
        serde_yaml::to_string(&data).expect("Failed to serialize seed data")
    );
}

#[test]
fn test_typical_seed() {
    init_rng(SEED_FOR_RANDOMIZER);
    // Create root to serialize it to yaml and add to a template file
    let seed_root = VolumesRoot {
        volumes: vec![
            VolumeGenerator {
                volume_name: None,
                volume_type: VolumeType::Memory,
                databases: DatabasesTemplateType::DatabasesTemplate(WithCount::<
                    Database,
                    DatabaseGenerator,
                >::new(
                    10,
                    DatabaseGenerator {
                        database_name: None,
                        schemas: SchemasTemplateType::SchemasTemplate(WithCount::<
                            Schema,
                            SchemaGenerator,
                        >::new(
                            3,
                            SchemaGenerator {
                                schema_name: None,
                                tables: TablesTemplateType::TablesTemplate(WithCount::<
                                    Table,
                                    TableGenerator,
                                >::new(
                                    5,
                                    TableGenerator {
                                        name: None,
                                        columns: ColumnsTemplateType::ColumnsTemplate(WithCount::<
                                            Column,
                                            ColumnGenerator,
                                        >::new(
                                            10,
                                            ColumnGenerator { col_name: None },
                                        )),
                                    },
                                )),
                            },
                        )),
                    },
                )),
            },
            VolumeGenerator {
                volume_name: Some("my memory volume".to_string()),
                volume_type: VolumeType::Memory,
                databases: DatabasesTemplateType::DatabasesTemplate(WithCount::<
                    Database,
                    DatabaseGenerator,
                >::new(
                    1,
                    DatabaseGenerator {
                        database_name: Some("test".to_string()),
                        schemas: SchemasTemplateType::Schemas(vec![Schema {
                            schema_name: "bar".to_string(),
                            tables: vec![Table {
                                name: "quux".to_string(),
                                columns: vec![Column {
                                    col_name: "corge".to_string(),
                                    col_type: ColumnType::Number,
                                }],
                            }],
                        }]),
                    },
                )),
            },
            VolumeGenerator {
                volume_name: Some("empty file volume".to_string()),
                volume_type: VolumeType::File(FileVolume {
                    path: "/tmp/empty_file_volume".to_string(),
                }),
                databases: DatabasesTemplateType::Databases(vec![]),
            },
        ],
    };

    // Save output of ^^ this to typical_seed.yaml when changing code ^^

    eprintln!(
        "programmatically created typical seed template: \n{}",
        serde_yaml::to_string(&seed_root).expect("Failed to serialize seed template")
    );

    let seed_template =
        read_seed_template(SeedVariant::Typical).expect("Failed to read seed template");
    assert_eq!(seed_root, seed_template);

    let data = seed_template.generate();
    eprintln!(
        "generated seed data: \n{}",
        serde_yaml::to_string(&data).expect("Failed to serialize seed data")
    );
}

// #[test]
// fn test_seed_root() {
//     init_rng(SEED_FOR_RANDOMIZER);
//     let seed_root = read_seed_root_template().expect("Failed to read seed root");
//     eprintln!("{:#?}", seed_root);
// }
