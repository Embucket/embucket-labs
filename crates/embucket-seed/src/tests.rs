use crate::external_models::{FileVolume, VolumeType};
use crate::seed_generator::{WithCount, parse_seed_template};
use crate::seed_models::{
    ColumnGenerator, DatabaseGenerator, SchemaGenerator, SeedTemplateRoot, TableGenerator,
    VolumeGenerator,
};
use crate::seed_models::{
    ColumnsTemplateType, DatabasesTemplateType, SchemasTemplateType, TablesTemplateType,
};
use crate::static_seed_assets::SeedVariant;

use crate::seed_client::seed_database;
use crate::seed_models::{Column, ColumnType, Database, Schema, Table};
use api_ui::test_server::run_test_server_with_demo_auth;

#[tokio::test]
async fn test_seed_client() {
    let addr = run_test_server_with_demo_auth(
        "secret".to_string(),
        "user1".to_string(),
        "pass1".to_string(),
    );

    seed_database(
        addr,
        SeedVariant::Typical,
        "user1".to_string(),
        "pass1".to_string(),
    )
    .await;
}

#[test]
fn test_seed_templates_parseable() {
    parse_seed_template(SeedVariant::Minimal.seed_data())
        .expect("Failed to parse 'minimal' seed template");
    parse_seed_template(SeedVariant::Typical.seed_data())
        .expect("Failed to parse 'typical' seed template");
    parse_seed_template(SeedVariant::Extreme.seed_data())
        .expect("Failed to parse 'extreme' seed template");
}

#[test]
fn test_seed_roundtrip() {
    // Create root to serialize it to yaml and add to a template file
    let seed_root = SeedTemplateRoot {
        volumes: vec![
            VolumeGenerator {
                volume_name: None,
                volume_type: VolumeType::Memory,
                databases: DatabasesTemplateType::DatabasesTemplate(WithCount::<
                    Database,
                    DatabaseGenerator,
                >::new(
                    2,
                    DatabaseGenerator {
                        database_name: None,
                        schemas: SchemasTemplateType::SchemasTemplate(WithCount::<
                            Schema,
                            SchemaGenerator,
                        >::new(
                            2,
                            SchemaGenerator {
                                schema_name: None,
                                tables: TablesTemplateType::TablesTemplate(WithCount::<
                                    Table,
                                    TableGenerator,
                                >::new(
                                    2,
                                    TableGenerator {
                                        table_name: None,
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
                                table_name: "quux".to_string(),
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

    let yaml_serialized =
        serde_yaml::to_string(&seed_root).expect("Failed to serialize seed template");

    eprintln!("programmatically created typical seed template: \n{yaml_serialized}");

    let seed_template =
        parse_seed_template(&yaml_serialized).expect("Failed to read seed template");
    assert_eq!(seed_root, seed_template);

    // just check it is not failing
    let res = seed_template.generate();
    assert_ne!(res.len(), 0);
}
