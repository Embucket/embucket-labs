use crate::query::QueryContext;
use crate::service::{CoreExecutionService, ExecutionService};
use crate::utils::{Config, DataSerializationFormat};
use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::models::table::TableIdent as MetastoreTableIdent;
use core_metastore::{
    Database as MetastoreDatabase, Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent,
    Volume as MetastoreVolume,
};
use datafusion::{arrow::csv::reader::Format, assert_batches_eq};

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_execute_always_returns_schema() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        Config {
            dbt_serialization_format: DataSerializationFormat::Json,
        },
    );

    execution_svc
        .create_session("test_session_id".to_string())
        .await
        .expect("Failed to create session");

    let (_, columns) = execution_svc
        .query(
            "test_session_id",
            "SELECT 1 AS a, 2.0 AS b, '3' AS c WHERE False",
            QueryContext::default(),
        )
        .await
        .expect("Failed to execute query");
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].r#type, "fixed");
    assert_eq!(columns[1].r#type, "real");
    assert_eq!(columns[2].r#type, "text");
}

#[tokio::test]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_service_upload_file() {
    let metastore = SlateDBMetastore::new_in_memory().await;

    metastore
        .create_volume(
            &"test_volume".to_string(),
            MetastoreVolume::new(
                "test_volume".to_string(),
                core_metastore::VolumeType::Memory,
            ),
        )
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(
            &"embucket".to_string(),
            MetastoreDatabase {
                ident: "embucket".to_string(),
                properties: None,
                volume: "test_volume".to_string(),
            },
        )
        .await
        .expect("Failed to create database");
    let schema_ident = MetastoreSchemaIdent {
        database: "embucket".to_string(),
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

    let file_name = "test.csv";
    let table_ident = MetastoreTableIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
        table: "target_table".to_string(),
    };

    // Create CSV data in memory
    let csv_content = "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300";
    let data = csv_content.as_bytes().to_vec();

    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        Config {
            dbt_serialization_format: DataSerializationFormat::Json,
        },
    );

    let session_id = "test_session_id";
    execution_svc
        .create_session(session_id.to_string())
        .await
        .expect("Failed to create session");

    let csv_format = Format::default().with_header(true);
    let rows_loaded = execution_svc
        .upload_data_to_table(
            session_id,
            &table_ident,
            data.clone().into(),
            file_name,
            csv_format.clone(),
        )
        .await
        .expect("Failed to upload file");
    assert_eq!(rows_loaded, 3);

    // Verify that the file was uploaded successfully by running select * from the table
    let query = format!("SELECT * FROM {}", table_ident.table);
    let (rows, _) = execution_svc
        .query(session_id, &query, QueryContext::default())
        .await
        .expect("Failed to execute query");

    assert_batches_eq!(
        &[
            "+----+-------+-------+",
            "| id | name  | value |",
            "+----+-------+-------+",
            "| 1  | test1 | 100   |",
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "+----+-------+-------+",
        ],
        &rows
    );

    let rows_loaded = execution_svc
        .upload_data_to_table(session_id, &table_ident, data.into(), file_name, csv_format)
        .await
        .expect("Failed to upload file");
    assert_eq!(rows_loaded, 3);

    // Verify that the file was uploaded successfully by running select * from the table
    let query = format!("SELECT * FROM {}", table_ident.table);
    let (rows, _) = execution_svc
        .query(session_id, &query, QueryContext::default())
        .await
        .expect("Failed to execute query");

    assert_batches_eq!(
        &[
            "+----+-------+-------+",
            "| id | name  | value |",
            "+----+-------+-------+",
            "| 1  | test1 | 100   |",
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "| 1  | test1 | 100   |",
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "+----+-------+-------+",
        ],
        &rows
    );
}

#[tokio::test]
async fn test_service_create_table_file_volume() {
    let metastore = SlateDBMetastore::new_in_memory().await;

    // Create a temporary directory for the file volume
    let temp_dir = std::env::temp_dir().join("test_file_volume");
    let _ = std::fs::create_dir_all(&temp_dir);
    let temp_path = temp_dir.to_str().expect("Failed to convert path to string");
    metastore
        .create_volume(
            &"test_volume".to_string(),
            MetastoreVolume::new(
                "test_volume".to_string(),
                core_metastore::VolumeType::File(core_metastore::FileVolume {
                    path: temp_path.to_string(),
                }),
            ),
        )
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(
            &"embucket".to_string(),
            MetastoreDatabase {
                ident: "embucket".to_string(),
                properties: None,
                volume: "test_volume".to_string(),
            },
        )
        .await
        .expect("Failed to create database");
    let schema_ident = MetastoreSchemaIdent {
        database: "embucket".to_string(),
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

    let table_ident = MetastoreTableIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
        table: "target_table".to_string(),
    };
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        Config {
            dbt_serialization_format: DataSerializationFormat::Json,
        },
    );
    let session_id = "test_session_id";
    execution_svc
        .create_session(session_id.to_string())
        .await
        .expect("Failed to create session");

    let create_table_sql = format!(
        "CREATE TABLE {table_ident} (id INT, name STRING, value FLOAT) as VALUES (1, 'test1', 100.0), (2, 'test2', 200.0), (3, 'test3', 300.0)"
    );
    let (res, _) = execution_svc
        .query(session_id, &create_table_sql, QueryContext::default())
        .await
        .expect("Failed to create table");

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 3     |",
            "+-------+",
        ],
        &res
    );

    let insert_sql = format!(
        "INSERT INTO {table_ident} (id, name, value) VALUES (4, 'test4', 400.0), (5, 'test5', 500.0)"
    );
    let (res, _) = execution_svc
        .query(session_id, &insert_sql, QueryContext::default())
        .await
        .expect("Failed to insert data");

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 2     |",
            "+-------+",
        ],
        &res
    );
}
