#![allow(clippy::expect_used)]
#![allow(clippy::wildcard_imports)]

use super::*;
use crate::models::*;
use crate::{
    Metastore,
    models::{
        database::Database,
        schema::{Schema, SchemaIdent},
        table::{TableCreateRequest, TableIdent},
        volumes::Volume,
    },
};
use futures::StreamExt;
use iceberg_rust_spec::{
    schema::Schema as IcebergSchema,
    types::{PrimitiveType, StructField, Type},
};
use std::result::Result;

use core_utils::scan_iterator::ScanIterator;
use object_store::ObjectStore;

fn insta_filters() -> Vec<(&'static str, &'static str)> {
    vec![
        (r"created_at[^,]*", "created_at: \"TIMESTAMP\""),
        (r"updated_at[^,]*", "updated_at: \"TIMESTAMP\""),
        (r"last_modified[^,]*", "last_modified: \"TIMESTAMP\""),
        (r"size[^,]*", "size: \"INTEGER\""),
        (r"last_updated_ms[^,]*", "last_update_ms: \"INTEGER\""),
        (
            r"[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
            "UUID",
        ),
        (r"lookup: \{[^}]*\}", "lookup: {LOOKUPS}"),
        (r"properties: \{[^}]*\}", "properties: {PROPERTIES}"),
        (r"at .*.rs:\d+:\d+", "at file:line:col"), // remove Error location
    ]
}

async fn get_metastore() -> SlateDBMetastore {
    SlateDBMetastore::new_in_memory().await
}

#[tokio::test]
async fn test_create_volumes() {
    let ms = get_metastore().await;

    let volume = Volume::new("test".to_owned(), VolumeType::Memory);
    let volume_id = volume.ident.clone();
    ms.create_volume(volume)
        .await
        .expect("create volume failed");
    let all_volumes = ms
        .get_volumes(ListParams::default())
        .await
        .expect("list volumes failed");

    let test_volume = ms
        .get_volume(&volume_id)
        .await
        .expect("get test volume failed");

    insta::with_settings!({
        filters => insta_filters(),
    }, {
        insta::assert_debug_snapshot!((test_volume, all_volumes));
    });
}

#[tokio::test]
async fn test_create_s3table_volume() {
    let ms = get_metastore().await;

    let s3table_volume = VolumeType::S3Tables(S3TablesVolume {
        arn: "arn:aws:s3tables:us-east-1:111122223333:bucket/my-table-bucket".to_string(),
        endpoint: Some("https://my-bucket-name.s3.us-east-1.amazonaws.com/".to_string()),
        credentials: AwsCredentials::AccessKey(AwsAccessKeyCredentials {
            aws_access_key_id: "kPYGGu34jF685erC7gst".to_string(),
            aws_secret_access_key: "Q2ClWJgwIZLcX4IE2zO2GBl8qXz7g4knqwLwUpWL".to_string(),
        }),
    });
    let volume = Volume::new("s3tables".to_string(), s3table_volume);
    ms.create_volume(volume.clone())
        .await
        .expect("create s3table volume failed");

    let created_volume = ms
        .get_volume(&volume.ident)
        .await
        .expect("get s3table volume failed");
    let created_volume = created_volume.expect("No volume in Option").data;

    insta::with_settings!({
        filters => insta_filters(),
    }, {
        insta::assert_debug_snapshot!((volume, created_volume));
    });
}

#[tokio::test]
async fn test_duplicate_volume() {
    let ms = get_metastore().await;

    let volume = Volume::new("test".to_owned(), VolumeType::Memory);
    ms.create_volume(volume)
        .await
        .expect("create volume failed");

    let volume2 = Volume::new("test".to_owned(), VolumeType::Memory);
    let result = ms.create_volume(volume2).await;
    insta::with_settings!({
        filters => insta_filters(),
    }, {
        insta::assert_debug_snapshot!(result);
    });
}

#[tokio::test]
async fn test_delete_volume() {
    let ms = get_metastore().await;

    let volume = Volume::new("test".to_owned(), VolumeType::Memory);
    ms.create_volume(volume.clone())
        .await
        .expect("create volume failed");
    let all_volumes = ms
        .get_volumes(ListParams::default())
        .await
        .expect("list volumes failed");
    let get_volume = ms
        .get_volume(&volume.ident)
        .await
        .expect("get volume failed");
    ms.delete_volume(&volume.ident, false)
        .await
        .expect("delete volume failed");
    let all_volumes_after = ms
        .get_volumes(ListParams::default())
        .await
        .expect("list volumes failed");

    insta::with_settings!({
        filters => insta_filters(),
    }, {
        insta::assert_debug_snapshot!((all_volumes, get_volume, all_volumes_after ));
    });
}

#[tokio::test]
async fn test_update_volume() {
    let ms = get_metastore().await;

    let volume = Volume::new("test".to_owned(), VolumeType::Memory);
    let rwo1 = ms
        .create_volume(volume.clone())
        .await
        .expect("create volume failed");
    let volume = Volume::new(
        "test".to_owned(),
        VolumeType::File(FileVolume {
            path: "/tmp".to_owned(),
        }),
    );
    let rwo2 = ms
        .update_volume(&"test".to_owned(), volume)
        .await
        .expect("update volume failed");
    insta::with_settings!({
        filters => insta_filters(),
    }, {
        insta::assert_debug_snapshot!((rwo1, rwo2));
    });
}

#[tokio::test]
async fn test_create_database() {
    let ms = get_metastore().await;
    let mut database = Database::new("testdb".to_owned(), "non_existing".to_owned());
    let no_volume_result = ms
        .create_database(database.clone())
        .await
        .expect_err("create database with non existing volume should fail");

    let volume_testv1 = ms
        .create_volume(Volume::new("testv1".to_owned(), VolumeType::Memory))
        .await
        .expect("create volume failed");

    database.volume = volume_testv1.ident.clone();
    ms.create_database(database.clone())
        .await
        .expect("create database failed");
    let all_databases = ms
        .get_databases(ListParams::default())
        .await
        .expect("list databases failed");

    // tests rename
    database.ident = "updated_testdb".to_owned();
    ms.update_database(&"testdb".to_owned(), database)
        .await
        .expect("update database failed");
    let fetched_db = ms
        .get_database(&"updated_testdb".to_owned())
        .await
        .expect("get database failed");

    ms.delete_database(&"updated_testdb".to_string(), false)
        .await
        .expect("delete database failed");
    let all_dbs_after = ms
        .get_databases(ListParams::default())
        .await
        .expect("list databases failed");

    insta::with_settings!({
        filters => insta_filters(),
    }, {
        insta::assert_debug_snapshot!((no_volume_result, all_databases, fetched_db, all_dbs_after));
    });
}

#[tokio::test]
async fn test_schemas() {
    let ms = get_metastore().await;
    let schema = Schema {
        ident: SchemaIdent {
            database: "testdb".to_owned(),
            schema: "testschema".to_owned(),
        },
        properties: None,
    };

    let no_db_result = ms
        .create_schema(&schema.ident.clone(), schema.clone())
        .await;

    let volume = ms
        .create_volume(Volume::new("testv1".to_owned(), VolumeType::Memory))
        .await
        .expect("create volume failed");
    ms.create_database(Database::new("testdb".to_owned(), volume.ident.clone()))
        .await
        .expect("create database failed");
    let schema_create = ms
        .create_schema(&schema.ident.clone(), schema.clone())
        .await
        .expect("create schema failed");

    let schema_list = ms
        .get_schemas(ListParams::default().by_parent_name(schema.ident.database.clone()))
        .await
        .expect("list schemas failed");
    let schema_get = ms
        .get_schema(&schema.ident)
        .await
        .expect("get schema failed");
    ms.delete_schema(&schema.ident, false)
        .await
        .expect("delete schema failed");
    let schema_list_after = ms
        .get_schemas(ListParams::default().by_parent_name(schema.ident.database))
        .await
        .expect("list schemas failed");

    insta::with_settings!({
        filters => insta_filters(),
    }, {
        insta::assert_debug_snapshot!((no_db_result, schema_create, schema_list, schema_get, schema_list_after));
    });
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_tables() {
    let ms = get_metastore().await;

    let schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_struct_field(StructField::new(
            0,
            "id",
            true,
            Type::Primitive(PrimitiveType::Int),
            None,
        ))
        .with_struct_field(StructField::new(
            1,
            "name",
            true,
            Type::Primitive(PrimitiveType::String),
            None,
        ))
        .build()
        .expect("schema build failed");

    let table = TableCreateRequest {
        ident: TableIdent {
            database: "testdb".to_owned(),
            schema: "testschema".to_owned(),
            table: "testtable".to_owned(),
        },
        format: None,
        properties: None,
        location: None,
        schema,
        partition_spec: None,
        sort_order: None,
        stage_create: None,
        volume_ident: None,
        is_temporary: None,
    };

    let no_schema_result = ms.create_table(&table.ident.clone(), table.clone()).await;

    let volume = Volume::new("testv1".to_owned(), VolumeType::Memory);
    let volume = ms
        .create_volume(volume)
        .await
        .expect("create volume failed");
    ms.create_database(Database::new("testdb".to_owned(), volume.ident.clone()))
        .await
        .expect("create database failed");
    ms.create_schema(
        &SchemaIdent {
            database: "testdb".to_owned(),
            schema: "testschema".to_owned(),
        },
        Schema {
            ident: SchemaIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
            },
            properties: None,
        },
    )
    .await
    .expect("create schema failed");
    let table_create = ms
        .create_table(&table.ident.clone(), table.clone())
        .await
        .expect("create table failed");
    let vol_object_store = ms
        .volume_object_store(volume.id().expect("Volume id not defined"))
        .await
        .expect("get volume object store failed")
        .expect("Object store not found");
    let paths: Result<Vec<_>, ()> = vol_object_store
        .list(None)
        .then(|c| async move { Ok::<_, ()>(c) })
        .collect::<Vec<Result<_, _>>>()
        .await
        .into_iter()
        .collect();

    let table_list = ms
        .iter_tables(&table.ident.clone().into())
        .collect()
        .await
        .expect("list tables failed");
    let table_get = ms.get_table(&table.ident).await.expect("get table failed");
    ms.delete_table(&table.ident, false)
        .await
        .expect("delete table failed");
    let table_list_after = ms
        .iter_tables(&table.ident.into())
        .collect()
        .await
        .expect("list tables failed");

    insta::with_settings!({
        filters => insta_filters(),
    }, {
        insta::assert_debug_snapshot!(
            (
                no_schema_result,
                table_create,
                paths,
                table_list,
                table_get,
                table_list_after
            )
        );
    });
}

#[tokio::test]
async fn test_temporary_tables() {
    let ms = get_metastore().await;

    let schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_struct_field(StructField::new(
            0,
            "id",
            true,
            Type::Primitive(PrimitiveType::Int),
            None,
        ))
        .with_struct_field(StructField::new(
            1,
            "name",
            true,
            Type::Primitive(PrimitiveType::String),
            None,
        ))
        .build()
        .expect("schema build failed");

    let table = TableCreateRequest {
        ident: TableIdent {
            database: "testdb".to_owned(),
            schema: "testschema".to_owned(),
            table: "testtable".to_owned(),
        },
        format: None,
        properties: None,
        location: None,
        schema,
        partition_spec: None,
        sort_order: None,
        stage_create: None,
        volume_ident: None,
        is_temporary: Some(true),
    };

    let volume = Volume::new("testv1".to_owned(), VolumeType::Memory);
    let volume = ms
        .create_volume(volume)
        .await
        .expect("create volume failed");
    ms.create_database(Database::new("testdb".to_owned(), volume.ident.clone()))
        .await
        .expect("create database failed");
    ms.create_schema(
        &SchemaIdent {
            database: "testdb".to_owned(),
            schema: "testschema".to_owned(),
        },
        Schema {
            ident: SchemaIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
            },
            properties: None,
        },
    )
    .await
    .expect("create schema failed");
    let create_table = ms
        .create_table(&table.ident.clone(), table.clone())
        .await
        .expect("create table failed");
    let vol_object_store = ms
        .table_object_store(&create_table.ident)
        .await
        .expect("get table object store failed")
        .expect("Object store not found");

    let paths: Result<Vec<_>, ()> = vol_object_store
        .list(None)
        .then(|c| async move { Ok::<_, ()>(c) })
        .collect::<Vec<Result<_, _>>>()
        .await
        .into_iter()
        .collect();

    insta::with_settings!({
        filters => insta_filters(),
    }, {
        insta::assert_debug_snapshot!((create_table.volume_ident.as_ref(), paths));
    });
}

// TODO: Add custom table location tests
