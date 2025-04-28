use std::sync::Arc;

use embucket_metastore::{
    Database as MetastoreDatabase, Metastore, Schema as MetastoreSchema,
    SchemaIdent as MetastoreSchemaIdent, SlateDBMetastore, Volume as MetastoreVolume,
};

use crate::execution::{query::QueryContext, session::UserSession};

#[tokio::test]
#[allow(clippy::expect_used, clippy::manual_let_else, clippy::too_many_lines)]
async fn test_create_table_and_insert() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    metastore
        .create_volume(
            &"test_volume".to_string(),
            MetastoreVolume::new(
                "test_volume".to_string(),
                embucket_metastore::VolumeType::Memory,
            ),
        )
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(
            &"benchmark".to_string(),
            MetastoreDatabase {
                ident: "benchmark".to_string(),
                properties: None,
                volume: "test_volume".to_string(),
            },
        )
        .await
        .expect("Failed to create database");
    let schema_ident = MetastoreSchemaIdent {
        database: "benchmark".to_string(),
        schema: "public".to_string(),
    };
    metastore
        .create_schema(
            &schema_ident.clone(),
            MetastoreSchema {
                ident: schema_ident,
                properties: None,
            },
        )
        .await
        .expect("Failed to create schema");
    let session = Arc::new(
        UserSession::new(metastore)
            .await
            .expect("Failed to create user session"),
    );
    let create_query = r"
        CREATE TABLE benchmark.public.hits
        (
            WatchID BIGINT NOT NULL,
            JavaEnable INTEGER NOT NULL,
            Title TEXT NOT NULL,
            GoodEvent INTEGER NOT NULL,
            EventTime BIGINT NOT NULL,
            EventDate INTEGER NOT NULL,
            CounterID INTEGER NOT NULL,
            ClientIP INTEGER NOT NULL,
            PRIMARY KEY (CounterID, EventDate, EventTime, WatchID)
        );
    ";
    let mut query1 = session.query(create_query, QueryContext::default());

    let statement = query1.parse_query().expect("Failed to parse query");
    let result = query1.execute().await.expect("Failed to execute query");

    let all_query = session
        .query("SHOW TABLES", QueryContext::default())
        .execute()
        .await
        .expect("Failed to execute query");

    let insert_query = session
        .query(
            "INSERT INTO benchmark.public.hits VALUES (1, 1, 'test', 1, 1, 1, 1, 1)",
            QueryContext::default(),
        )
        .execute()
        .await
        .expect("Failed to execute query");

    let select_query = session
        .query(
            "SELECT * FROM benchmark.public.hits",
            QueryContext::default(),
        )
        .execute()
        .await
        .expect("Failed to execute query");

    insta::assert_debug_snapshot!((statement, result, all_query, insert_query, select_query));
}
