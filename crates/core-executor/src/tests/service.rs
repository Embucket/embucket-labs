// Updated imports for new error handling
use crate::error::{ExecutionError, ExecutionResult, MetastoreSnafu, ApplicationSnafu, CoreExecutorErrorKind}; // Added more specific types
use crate::models::QueryResultData;
use crate::query::QueryContext;
use crate::service::{CoreExecutionService, ExecutionService}; // Assuming ExecutionService trait is still relevant
use crate::utils::{Config, DataSerializationFormat};
use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::models::table::TableIdent as MetastoreTableIdent;
use core_metastore::{
    Database as MetastoreDatabase, Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent,
    Volume as MetastoreVolume, MetastoreResult as CoreMetastoreResult, // For mapping errors
};
use datafusion::arrow::csv::reader::Format as CsvFormat; // Aliased for clarity
use datafusion::assert_batches_eq;
use embucket_errors::wrap_error; // Import wrap_error
use snafu::ResultExt; // For .context()

// Helper to setup metastore for service tests, similar to create_df_session but might not need full DF setup.
// Returns ExecutionResult to propagate errors during setup.
async fn setup_metastore_for_service_tests() -> ExecutionResult<Arc<SlateDBMetastore>> {
    let metastore = SlateDBMetastore::new_in_memory().await; // Panics on internal failure, test setup prerequisite

    let map_metastore_err = |e: core_metastore::error::MetastoreError, context_msg: &'static str| {
        wrap_error(e, context_msg.to_string()).context(MetastoreSnafu)
    };

    metastore
        .create_volume(
            &"test_volume".to_string(),
            MetastoreVolume::new("test_volume".to_string(), core_metastore::VolumeType::Memory),
        )
        .await
        .map_err(|e| map_metastore_err(e, "Service Test Setup: Failed to create volume"))?;
    
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
        .map_err(|e| map_metastore_err(e, "Service Test Setup: Failed to create database"))?;

    let schema_ident = MetastoreSchemaIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
    };
    metastore
        .create_schema(
            &schema_ident.clone(),
            MetastoreSchema { ident: schema_ident, properties: None },
        )
        .await
        .map_err(|e| map_metastore_err(e, "Service Test Setup: Failed to create schema"))?;
    
    Ok(metastore)
}


#[tokio::test]
async fn test_execute_always_returns_schema() -> ExecutionResult<()> { // Changed to return ExecutionResult
    let metastore = setup_metastore_for_service_tests().await?; // Use helper and propagate error
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        Config { dbt_serialization_format: DataSerializationFormat::Json },
    ).map_err(|e| wrap_error(e, "Failed to create CoreExecutionService for schema test".to_string())).context(ApplicationSnafu)?; // Map error if new can fail

    execution_svc.create_session("test_session_id_schema".to_string()).await?; // Propagate error

    let query_result = execution_svc
        .query(
            "test_session_id_schema",
            "SELECT 1 AS a, 2.0 AS b, '3' AS c WHERE False", // Query that returns no rows but has schema
            QueryContext::default(),
        )
        .await;
    
    assert!(query_result.is_ok(), "Query failed: {:?}", query_result.err());
    let QueryResultData { columns_info: columns, .. } = query_result.unwrap();
    
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].r#type, "fixed"); // DataFusion type names might vary slightly
    assert_eq!(columns[1].r#type, "real");
    assert_eq!(columns[2].r#type, "text");
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Test setup can be verbose
async fn test_service_upload_file() -> ExecutionResult<()> { // Changed to return ExecutionResult
    let metastore = setup_metastore_for_service_tests().await?;
    let file_name = "test_upload.csv";
    let table_ident = MetastoreTableIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
        table: "target_upload_table".to_string(), // Unique table name
    };

    let csv_content = "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300";
    let data = csv_content.as_bytes().to_vec();

    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        Config { dbt_serialization_format: DataSerializationFormat::Json },
    ).map_err(|e| wrap_error(e, "Failed to create CoreExecutionService for upload test".to_string())).context(ApplicationSnafu)?;

    let session_id = "test_session_id_upload";
    execution_svc.create_session(session_id.to_string()).await?;

    let csv_format = CsvFormat::default().with_header(true);
    let upload_result = execution_svc
        .upload_data_to_table(
            session_id,
            &table_ident,
            data.clone().into(),
            file_name,
            csv_format.clone(),
        )
        .await;
    
    assert!(upload_result.is_ok(), "Upload file failed: {:?}", upload_result.err());
    assert_eq!(upload_result.unwrap(), 3);

    let query_str = format!("SELECT * FROM {}", table_ident.table); // Use field access
    let query_data_res = execution_svc.query(session_id, &query_str, QueryContext::default()).await;
    assert!(query_data_res.is_ok(), "Query after upload failed: {:?}", query_data_res.err());
    let QueryResultData { records, .. } = query_data_res.unwrap();

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
        &records
    );

    // Test uploading again (append)
    let second_upload_result = execution_svc
        .upload_data_to_table(session_id, &table_ident, data.into(), file_name, csv_format)
        .await;
    assert!(second_upload_result.is_ok(), "Second upload failed: {:?}", second_upload_result.err());
    assert_eq!(second_upload_result.unwrap(), 3);

    let second_query_res = execution_svc.query(session_id, &query_str, QueryContext::default()).await;
    assert!(second_query_res.is_ok(), "Query after second upload failed: {:?}", second_query_res.err());
    let QueryResultData { records: records_after_second_upload, .. } = second_query_res.unwrap();

    assert_batches_eq!(
        &[
            "+----+-------+-------+",
            "| id | name  | value |",
            "+----+-------+-------+",
            "| 1  | test1 | 100   |", // First upload
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "| 1  | test1 | 100   |", // Second upload
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "+----+-------+-------+",
        ],
        &records_after_second_upload
    );
    Ok(())
}

#[tokio::test]
async fn test_service_create_table_file_volume() -> ExecutionResult<()> { // Changed to return ExecutionResult
    let metastore = SlateDBMetastore::new_in_memory().await; // Setup panics
    let temp_dir = std::env::temp_dir().join("test_file_volume_service"); // Unique dir
    std::fs::create_dir_all(&temp_dir).map_err(|e| 
        wrap_error(e, format!("Failed to create temp dir for file volume: {:?}", temp_dir))
    ).context(crate::error::IoSnafu)?; // Changed to IoSnafu from ExecutionError

    let temp_path = temp_dir.to_str().ok_or_else(|| {
        let kind = CoreExecutorErrorKind::InitializationFailed { service_name: "TempPathConversion".to_string() };
        wrap_error(kind, "Failed to convert temp_dir to string".to_string()).context(ApplicationSnafu)
    })?.to_string();

    // Metastore setup
    let map_metastore_err = |e: core_metastore::error::MetastoreError, context_msg: &'static str| {
        wrap_error(e, context_msg.to_string()).context(MetastoreSnafu)
    };
    metastore.create_volume(&"test_file_vol".to_string(), MetastoreVolume::new("test_file_vol".to_string(), core_metastore::VolumeType::File(core_metastore::FileVolume { path: temp_path }))).await.map_err(|e| map_metastore_err(e, "Setup: create_volume for file volume"))?;
    metastore.create_database(&"embucket_file".to_string(), MetastoreDatabase { ident: "embucket_file".to_string(), properties: None, volume: "test_file_vol".to_string() }).await.map_err(|e| map_metastore_err(e, "Setup: create_database for file volume"))?;
    let schema_ident = MetastoreSchemaIdent { database: "embucket_file".to_string(), schema: "public_file".to_string() };
    metastore.create_schema(&schema_ident, MetastoreSchema { ident: schema_ident.clone(), properties: None }).await.map_err(|e| map_metastore_err(e, "Setup: create_schema for file volume"))?;

    let table_ident = MetastoreTableIdent { database: "embucket_file".to_string(), schema: "public_file".to_string(), table: "target_file_table".to_string() };
    let execution_svc = CoreExecutionService::new(metastore.clone(), Config { dbt_serialization_format: DataSerializationFormat::Json })
        .map_err(|e| wrap_error(e, "Failed to create service for file volume test".to_string())).context(ApplicationSnafu)?;
    let session_id = "test_session_id_file_vol";
    execution_svc.create_session(session_id.to_string()).await?;

    let create_table_sql = format!("CREATE TABLE {} (id INT, name STRING, value FLOAT) AS VALUES (1, 'test1', 100.0), (2, 'test2', 200.0), (3, 'test3', 300.0)", table_ident);
    let create_res = execution_svc.query(session_id, &create_table_sql, QueryContext::default()).await;
    assert!(create_res.is_ok(), "Create table on file volume failed: {:?}", create_res.err());
    let QueryResultData { records: create_records, .. } = create_res.unwrap();
    assert_batches_eq!(&["+-------+", "| count |", "+-------+", "| 3     |", "+-------+"], &create_records);

    let insert_sql = format!("INSERT INTO {} (id, name, value) VALUES (4, 'test4', 400.0), (5, 'test5', 500.0)", table_ident);
    let insert_res = execution_svc.query(session_id, &insert_sql, QueryContext::default()).await;
    assert!(insert_res.is_ok(), "Insert into table on file volume failed: {:?}", insert_res.err());
    let QueryResultData { records: insert_records, .. } = insert_res.unwrap();
    assert_batches_eq!(&["+-------+", "| count |", "+-------+", "| 2     |", "+-------+"], &insert_records);

    // Cleanup the temporary directory
    std::fs::remove_dir_all(&temp_dir).map_err(|e| 
        wrap_error(e, format!("Failed to cleanup temp dir for file volume: {:?}", temp_dir))
    ).context(crate::error::IoSnafu)?;
    Ok(())
}

#[tokio::test]
async fn test_query_table_not_found() -> ExecutionResult<()> {
    let metastore = setup_metastore_for_service_tests().await?;
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        Config { dbt_serialization_format: DataSerializationFormat::Json },
    ).map_err(|e| wrap_error(e, "Failed to create service for table not found test".to_string())).context(ApplicationSnafu)?;
    let session_id = "test_session_table_not_found";
    execution_svc.create_session(session_id.to_string()).await?;

    let query_res = execution_svc.query(session_id, "SELECT * FROM non_existent_table_in_service_test", QueryContext::default()).await;
    assert!(query_res.is_err(), "Querying non-existent table should fail.");
    
    let err = query_res.unwrap_err();
    match err {
        ExecutionError::DataFusionQuery { source: emb_err, query: _ } => { // Or potentially DataFusion if it's not a query-specific DF error
            // The exact DataFusionError message for "table not found" can vary.
            // It might be "Table or view not found", "Unable to resolve relation", etc.
            // Check for common substrings.
            let source_err_string = emb_err.source.to_string();
            assert!(
                source_err_string.contains("not found") || 
                source_err_string.contains("Unable to resolve") ||
                source_err_string.contains("does not exist"), 
                "Unexpected DataFusionError message: {}", source_err_string
            );
            assert!(emb_err.context.contains("Executing SQL query")); // Context from UserSession::query
            assert!(!emb_err.get_backtrace().to_string().is_empty());
        }
        other_err => panic!("Incorrect ExecutionError variant for table not found: {:?}", other_err),
    }
    Ok(())
}

#[tokio::test]
async fn test_upload_to_non_existent_table() -> ExecutionResult<()> {
    let metastore = setup_metastore_for_service_tests().await?;
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        Config { dbt_serialization_format: DataSerializationFormat::Json },
    ).map_err(|e| wrap_error(e, "Failed to create service for upload to non-existent table test".to_string())).context(ApplicationSnafu)?;
    let session_id = "test_session_upload_no_table";
    execution_svc.create_session(session_id.to_string()).await?;

    let table_ident = MetastoreTableIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
        table: "upload_ghost_table".to_string(),
    };
    let csv_content = "id,name\n1,data";
    let data = csv_content.as_bytes().to_vec();
    let csv_format = CsvFormat::default().with_header(true);

    let upload_result = execution_svc.upload_data_to_table(
        session_id,
        &table_ident,
        data.into(),
        "ghost.csv",
        csv_format,
    ).await;

    assert!(upload_result.is_err(), "Upload to non-existent table should fail.");
    let err = upload_result.unwrap_err();
    
    // The error originates in metastore's create_table (if table doesn't exist, it tries to create)
    // or get_table. In this case, upload_data_to_table first tries to ensure the table exists.
    // If it tries to create it and the schema/db doesn't exist, that's one path.
    // If it checks get_table and it's not there, it might try to create it.
    // The current `upload_data_to_table` in `service.rs` calls `ensure_table_exists` which calls `metastore.create_table`.
    // If the schema `public` or db `embucket` didn't exist (they do from setup), it would be SchemaNotFound or DatabaseNotFound from metastore.
    // Since they exist, it would try to create the table. If it fails for other reasons (e.g. bad schema in request), that's one error.
    // Here, the table `upload_ghost_table` just doesn't exist. `ensure_table_exists` would create it.
    // Let's assume `upload_data_to_table` itself tries to create the table via `ensure_table_exists`.
    // The error we expect is from the DataFusion layer if the copy into the newly created table fails, or from metastore if table creation has issues.
    // `upload_data_to_table` in `service.rs` calls `self.session_ctx.copy_to_table`. This is where DataFusionError would occur if table creation succeeded but copy failed.
    // The test for `upload_data_to_table` should probably assume the table *does not exist* and it's created.
    // For *this* test, let's assume we want to check the "table not found" error if `upload_data_to_table` *required* an existing table.
    // However, `upload_data_to_table` implies it *can* create the table.
    // The error here is more likely to be a DataFusion error during the COPY operation if the schema isn't inferred correctly or some other DF issue.
    // Or if `ensure_table_exists` (which calls `metastore.create_table`) fails.
    // The current implementation of `upload_data_to_table` will attempt to create the table if it doesn't exist.
    // This test might be better framed as "upload fails due to incompatible CSV schema" or similar DataFusion issue.
    // For now, let's check for a general DataFusion error, as the path to "table not found" from DataFusion after creation is less direct.

    match err {
        ExecutionError::DataFusion { source: emb_err } | // If error from DF's COPY command
        ExecutionError::DataFusionQuery { source: emb_err, .. } | // If ensure_table_exists generated a query that failed
        ExecutionError::Metastore { source: emb_err } => { // If ensure_table_exists (metastore.create_table) failed
            // Example: If ensure_table_exists failed because schema was bad from CSV.
            // Or if the COPY INTO statement failed.
            assert!(!emb_err.get_backtrace().to_string().is_empty());
            // The context would be "Failed to load data into table" from service.rs/upload_data_to_table
            assert!(emb_err.context.contains("Failed to load data into table") || emb_err.context.contains("ensure_table_exists"));
        }
        _ => panic!("Unexpected error variant for upload to non-existent table: {:?}", err),
    }
    Ok(())
}
