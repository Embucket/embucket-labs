use crate::seed::rng::{SEED_FOR_RANDOMIZER, init_rng};
use crate::seed::{
    ColumnGenerator, DatabaseGenerator, SchemaGenerator, TableGenerator, VolumeGenerator,
    VolumesRoot, read_seed_template,
};
use crate::{SeedApi, SeedDatabase, SeedVariant};
use api_structs::volumes::{FileVolume, VolumeType};

use crate::seed::{
    Column, ColumnType, ColumnsTemplateType, Database, DatabasesTemplateType, Schema,
    SchemasTemplateType, Table, TablesTemplateType, WithCount,
};
use api_ui::test_server::run_test_server_with_demo_auth;

#[tokio::test]
async fn test_seed_client() {
    let addr =
        run_test_server_with_demo_auth(String::new(), "user1".to_string(), "pass1".to_string())
            .await;

    init_rng(SEED_FOR_RANDOMIZER);

    let mut seed_db = SeedDatabase::new(addr);
    seed_db.try_load_seed(SeedVariant::Typical).unwrap();
    seed_db
        .login("user1", "pass1")
        .await
        .expect("Failed to login");
    seed_db.seed_all().await.expect("Failed to seed database");
}

#[test]
fn test_super_gen() {
    init_rng(SEED_FOR_RANDOMIZER);
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
                volume_name: None,
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
        ],
    };

    // Save output of ^^ this to typical_seed.yaml when changing code ^^

    eprintln!(
        "programmatically created #1: \n{}",
        serde_yaml::to_string(&seed_root).unwrap()
    );

    let seed_gen = read_seed_template(SeedVariant::Typical).expect("Failed to read seed gen");
    eprintln!("{:#?}", seed_gen);
    assert_eq!(seed_root, seed_gen);

    eprintln!("loaded from file: {:#?}", seed_gen);
    let data = seed_gen.generate();
    eprintln!(
        "generated seed data: \n{}",
        serde_yaml::to_string(&data).unwrap()
    );
}

// #[test]
// fn test_seed_root() {
//     init_rng(SEED_FOR_RANDOMIZER);
//     let seed_root = read_seed_root_template().expect("Failed to read seed root");
//     eprintln!("{:#?}", seed_root);
// }
