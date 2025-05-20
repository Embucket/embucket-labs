use crate::seed::rng::{SEED_FOR_RANDOMIZER, init_rng};
use crate::seed::{
    read_volumes_template,
    VolumesRoot, VolumeGenerator, DatabaseGenerator, SchemaGenerator, TableGenerator, ColumnGenerator,
};
use crate::{SeedApi, SeedDatabase, SeedVariant};
use api_structs::volumes::{FileVolume, VolumeType};

use crate::seed::{DatabasesTemplateType, SchemasTemplateType, TablesTemplateType, ColumnsTemplateType,
    WithCount, Database, Schema, Table, Column, ColumnType,
};
use api_ui::test_server::run_test_server_with_demo_auth;

#[tokio::test]
async fn test_seed_client() {
    let addr = run_test_server_with_demo_auth(
        String::new(),
        "user1".to_string(),
        "pass1".to_string(),
    )
    .await;

    init_rng(SEED_FOR_RANDOMIZER);
    let seed_gen = read_volumes_template().expect("Failed to read seed gen");
    eprintln!("{:#?}", seed_gen);
    let mut seed_db = SeedDatabase::new(addr);
    seed_db.try_load_seed(SeedVariant::Minimal).unwrap();
    seed_db.login("user1", "pass1").await.expect("Failed to login");
    seed_db.create_volumes().await.expect("Failed to create volumes");
}

#[test]
fn test_super_gen() {
    init_rng(SEED_FOR_RANDOMIZER);
    let seed_root = VolumesRoot {
        volumes: vec![
            VolumeGenerator {
                volume_name: Some("test".to_string()),
                volume_type: VolumeType::Memory,
                databases: DatabasesTemplateType::DatabasesTemplate(
                    WithCount::<Database, DatabaseGenerator>::new(
                        2,
                        DatabaseGenerator {
                            database_name: Some("static".to_string()),
                            schemas: SchemasTemplateType::SchemasTemplate(
                                WithCount::<Schema, SchemaGenerator>::new(
                                    3,
                                    SchemaGenerator {
                                        schema_name: None,
                                        tables: TablesTemplateType::TablesTemplate(
                                            WithCount::<Table, TableGenerator>::new(
                                                5,
                                                TableGenerator {
                                                    name: None,
                                                    columns: ColumnsTemplateType::ColumnsTemplate(
                                                        WithCount::<Column, ColumnGenerator>::new(
                                                            10,
                                                            ColumnGenerator {
                                                                col_name: None,
                                                            }
                                                        )
                                                    ),
                                                }
                                            )
                                        )
                                    }
                                )
                            )
                        }
                    )
                )
            },
            VolumeGenerator {
                volume_name: None,
                volume_type: VolumeType::File(FileVolume {
                    path: "/tmp/seed_test".to_string(),
                }),
                databases: DatabasesTemplateType::DatabasesTemplate(
                    WithCount::<Database, DatabaseGenerator>::new(
                        2,
                        DatabaseGenerator {
                            database_name: None,
                            schemas: SchemasTemplateType::Schemas(vec![
                                Schema {
                                    schema_name: "bar".to_string(),
                                    tables: vec![
                                        Table {
                                            name: "quux".to_string(),
                                            columns: vec![
                                                Column {
                                                    col_name: "corge".to_string(),
                                                    col_type: ColumnType::Number,
                                                },
                                            ],
                                        },
                                    ],
                                },
                            ])
                        }
                    )
                )
            }
        ],
    };

    eprintln!(
        "programmatically created #1: \n{}",
        serde_yaml::to_string(&seed_root).unwrap()
    );

    let volumes_root = read_volumes_template().expect("Failed to read seed gen");
    eprintln!("loaded from file: {:#?}", volumes_root);
    let data = volumes_root.generate();
    eprintln!("generated seed data: \n{}", serde_yaml::to_string(&data).unwrap());
    assert!(false);
}

// #[test]
// fn test_seed_root() {
//     init_rng(SEED_FOR_RANDOMIZER);
//     let seed_root = read_seed_root_template().expect("Failed to read seed root");
//     eprintln!("{:#?}", seed_root);
// }
