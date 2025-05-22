use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hyper::body::to_bytes; 
use tower::ServiceExt; 

use crate::{
    error::{IcebergAPIError, ErrorResponse, ApiIcebergRestErrorKind}, 
    router::create_router,
    state::{AppState, Config as IcebergConfig},
    schemas::to_schema, // For create_namespace payload construction
};
use core_metastore::{
    error::{MetastoreError, MetastoreErrorKind, self as cm_error}, 
    Metastore as MetastoreTrait, 
    SchemaIdent as MetastoreSchemaIdent,
    models::{RwObject, Schema as MetastoreSchemaData, Volume as MetastoreVolumeData, Database as MetastoreDatabaseData, Table as MetastoreTableData, TableIdent as MetastoreTableIdent},
    scan_iterator::VecScanIterator, 
};
use core_utils::Error as CoreUtilsError; // For simulating UtilSlateDB errors
use embucket_errors::{wrap_error, EmbucketError}; 
use mockall::mock;
use std::sync::Arc;
use iceberg_rest_catalog::models::CreateNamespaceRequest; // For create_namespace test

// Define the mock Metastore (ensure all trait methods are included)
mock! {
    pub Metastore {}
    #[async_trait::async_trait]
    impl MetastoreTrait for Metastore {
        fn iter_volumes(&self) -> MetastoreResult<VecScanIterator<RwObject<MetastoreVolumeData>>> { unimplemented!() }
        async fn create_volume(&self, name: &str, volume: MetastoreVolumeData) -> MetastoreResult<RwObject<MetastoreVolumeData>> { unimplemented!() }
        async fn get_volume(&self, name: &str) -> MetastoreResult<Option<RwObject<MetastoreVolumeData>>> { unimplemented!() }
        async fn update_volume(&self, name: &str, volume: MetastoreVolumeData) -> MetastoreResult<RwObject<MetastoreVolumeData>> { unimplemented!() }
        async fn delete_volume(&self, name: &str, cascade: bool) -> MetastoreResult<()> { unimplemented!() }
        async fn volume_object_store(&self, name: &str) -> MetastoreResult<Arc<dyn object_store::ObjectStore>>; // Used by create_table etc.

        fn iter_databases(&self, volume_ident: Option<&str>) -> MetastoreResult<VecScanIterator<RwObject<MetastoreDatabaseData>>> { unimplemented!() }
        async fn create_database(&self, name: &str, database: MetastoreDatabaseData) -> MetastoreResult<RwObject<MetastoreDatabaseData>> { unimplemented!() }
        async fn get_database(&self, name: &str) -> MetastoreResult<Option<RwObject<MetastoreDatabaseData>>>;
        async fn update_database(&self, name: &str, database: MetastoreDatabaseData) -> MetastoreResult<RwObject<MetastoreDatabaseData>> { unimplemented!() }
        async fn delete_database(&self, name: &str, cascade: bool) -> MetastoreResult<()> { unimplemented!() }

        fn iter_schemas(&self, database: &str) -> MetastoreResult<VecScanIterator<RwObject<MetastoreSchemaData>>> { unimplemented!() }
        async fn create_schema(&self, ident: &MetastoreSchemaIdent, schema: MetastoreSchemaData) -> MetastoreResult<RwObject<MetastoreSchemaData>>;
        async fn get_schema(&self, ident: &MetastoreSchemaIdent) -> MetastoreResult<Option<RwObject<MetastoreSchemaData>>>;
        async fn update_schema(&self, ident: &MetastoreSchemaIdent, schema: MetastoreSchemaData) -> MetastoreResult<RwObject<MetastoreSchemaData>> { unimplemented!() }
        async fn delete_schema(&self, ident: &MetastoreSchemaIdent, cascade: bool) -> MetastoreResult<()> { unimplemented!() }

        fn iter_tables(&self, schema: &MetastoreSchemaIdent) -> MetastoreResult<VecScanIterator<RwObject<MetastoreTableData>>> { unimplemented!() }
        async fn create_table(&self, ident: &MetastoreTableIdent, table: core_metastore::TableCreateRequest) -> MetastoreResult<RwObject<MetastoreTableData>>; // For create_table validation test
        async fn get_table(&self, ident: &MetastoreTableIdent) -> MetastoreResult<Option<RwObject<MetastoreTableData>>> { unimplemented!() }
        async fn update_table(&self, ident: &MetastoreTableIdent, update: core_metastore::TableUpdate) -> MetastoreResult<RwObject<MetastoreTableData>> { unimplemented!() }
        async fn delete_table(&self, ident: &MetastoreTableIdent, cascade: bool) -> MetastoreResult<()> { unimplemented!() }
        async fn table_object_store(&self, ident: &MetastoreTableIdent) -> MetastoreResult<Arc<dyn object_store::ObjectStore>>;

        async fn table_exists(&self, ident: &MetastoreTableIdent) -> MetastoreResult<bool> { unimplemented!() }
        async fn url_for_table(&self, ident: &MetastoreTableIdent) -> MetastoreResult<String> { unimplemented!() }
        async fn volume_for_table(&self, ident: &MetastoreTableIdent) -> MetastoreResult<RwObject<MetastoreVolumeData>>;
    }
}

fn test_app(mock_metastore: Arc<MockMetastore>) -> axum::Router {
    let mock_config = Arc::new(IcebergConfig {
        iceberg_catalog_url: "http://localhost:8080".to_string(),
    });
    let mock_state = AppState {
        metastore: mock_metastore as Arc<dyn MetastoreTrait>,
        config: mock_config,
    };
    create_router().with_state(mock_state)
}

#[tokio::test]
async fn test_get_namespace_not_found() {
    let mut mock_metastore = MockMetastore::new();
    mock_metastore
        .expect_get_schema() // This is what get_namespace handler calls
        .withf(|ident: &MetastoreSchemaIdent| ident.database == "test_db" && ident.schema == "test_schema_not_found")
        .returning(|_ident| Ok(None)); // Metastore returns Ok(None) for not found

    let app = test_app(Arc::new(mock_metastore));
    let request = Request::builder()
        .uri("/v1/default/namespaces/test_db/schemas/test_schema_not_found") // Path changed to match router
        .method("GET")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body_bytes = to_bytes(response.into_body()).await.unwrap();
    let error_response: ErrorResponse = serde_json::from_slice(&body_bytes).expect("Failed to deserialize ErrorResponse");

    assert_eq!(error_response.error_type, "MetastoreOperationError");
    // Expected message from refactored handler:
    // "API: Namespace 'test_db.test_schema_not_found' not found: Schema does not exist in metastore: Schema 'test_schema_not_found' not found in database 'test_db'"
    assert!(error_response.message.contains("API: Namespace 'test_db.test_schema_not_found' not found"));
    assert!(error_response.message.contains("Schema does not exist in metastore"));
    assert!(error_response.message.contains("Schema 'test_schema_not_found' not found in database 'test_db'"));
    assert!(error_response.stack.is_none());
}

#[tokio::test]
async fn test_create_namespace_conflict() {
    let mut mock_metastore = MockMetastore::new();
    mock_metastore
        .expect_create_schema()
        .returning(|ident, _schema_data| {
            let kind = MetastoreErrorKind::SchemaAlreadyExists { db: ident.database.clone(), schema: ident.schema.clone() };
            let embucket_kind = wrap_error(kind, "Metastore: Schema already present.".to_string());
            Err(MetastoreError::OperationFailed{ source: embucket_kind })
        });

    let app = test_app(Arc::new(mock_metastore));
    // Correct payload structure for CreateNamespaceRequest
    let request_payload = CreateNamespaceRequest {
        namespace: vec!["test_db_conflict".to_string(), "existing_schema_conflict".to_string()],
        properties: None,
    };

    let request = Request::builder()
        .uri("/v1/default/namespaces") // POST to base, namespace in payload
        .method("POST")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&request_payload).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let body_bytes = to_bytes(response.into_body()).await.unwrap();
    let error_response: ErrorResponse = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(error_response.error_type, "MetastoreOperationError");
    // Expected: "API: Failed to create namespace 'test_db_conflict.existing_schema_conflict': Metastore: Schema already present.: Schema 'existing_schema_conflict' already exists in database 'test_db_conflict'"
    assert!(error_response.message.contains("API: Failed to create namespace 'test_db_conflict.existing_schema_conflict'"));
    assert!(error_response.message.contains("Metastore: Schema already present."));
    assert!(error_response.message.contains("Schema 'existing_schema_conflict' already exists in database 'test_db_conflict'"));
}

#[tokio::test]
async fn test_get_namespace_internal_server_error() {
    let mut mock_metastore = MockMetastore::new();
    mock_metastore
        .expect_get_schema()
        .returning(|_ident| {
            let core_utils_err = CoreUtilsError::Storage { message: "Simulated DB connection failure".to_string() };
            let embucket_core_err = wrap_error(core_utils_err, "Low-level storage access failed".to_string());
            Err(MetastoreError::UtilSlateDB { source: embucket_core_err })
        });

    let app = test_app(Arc::new(mock_metastore));
    let request = Request::builder()
        .uri("/v1/default/namespaces/any_db/schemas/any_schema")
        .method("GET")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body_bytes = to_bytes(response.into_body()).await.unwrap();
    let error_response: ErrorResponse = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(error_response.error_type, "MetastoreOperationError");
    // Expected: "API: Failed to query namespace 'any_db.any_schema': Low-level storage access failed: Simulated DB connection failure"
    assert!(error_response.message.contains("API: Failed to query namespace 'any_db.any_schema'"));
    assert!(error_response.message.contains("Low-level storage access failed"));
    assert!(error_response.message.contains("Simulated DB connection failure"));
}

#[tokio::test]
async fn test_create_namespace_invalid_payload_structure() {
    // This test doesn't need a mock metastore as the error should occur before metastore interaction.
    let mock_metastore = MockMetastore::new(); // Still need to create it for the app state
    let app = test_app(Arc::new(mock_metastore));

    // Payload with namespace not being a two-part array
    let request_payload = serde_json::json!({
        "namespace": ["too_short"], // Invalid structure for our handler's expectation
        "properties": {}
    });

    let request = Request::builder()
        .uri("/v1/default/namespaces")
        .method("POST")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&request_payload).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST); // Or as per ApiIcebergRestErrorKind mapping

    let body_bytes = to_bytes(response.into_body()).await.unwrap();
    let error_response: ErrorResponse = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(error_response.error_type, "RequestProcessingError"); // From IcebergAPIError::RequestProcessingFailed
    // Expected: "Invalid namespace structure in request: Invalid request parameter: namespace, reason: Namespace must be a two-part identifier (database.schema)."
    assert!(error_response.message.contains("Invalid namespace structure in request"));
    assert!(error_response.message.contains("Invalid request parameter: namespace"));
    assert!(error_response.message.contains("Namespace must be a two-part identifier (database.schema)."));
}

// Example for CreateTableRequest validation - requires a valid TableCreateRequest from core_metastore
// For this, we'd need to mock volume_for_table to avoid metastore calls during to_create_table.
// This test becomes more involved due to internal logic in to_create_table.
// A simpler validation test might be if CreateTableRequest itself had Validate derive.
// The current CreateTableRequest is from iceberg_rest_catalog::models, which might not have Validate.
// The handler calls `ib_create_table.validate().context(metastore_error::ValidationSnafu)?;`
// This `ib_create_table` is `core_metastore::TableCreateRequest`. We need to ensure this one has `Validate`.
// Assuming `core_metastore::TableCreateRequest` derives `Validate` and has some validation rules.
// For example, if `name` in `CreateTableRequest` (Iceberg model) was empty, and `to_create_table`
// preserved that, and `core_metastore::TableCreateRequest` validated `ident.table` as non-empty.

// Test for a CreateTableRequest that fails validation (e.g. empty table name)
#[tokio::test]
async fn test_create_table_validation_error() {
    let mut mock_metastore = MockMetastore::new();
    // Mock volume_for_table because create_table handler calls it.
    // It's called with the table ident derived from the request.
    mock_metastore.expect_volume_for_table()
        .returning(|_table_ident| {
            // Return a dummy volume, doesn't matter much as validation should fail before this is deeply used.
            let vol_data = MetastoreVolumeData { ident: "dummy_vol".to_string(), volume: core_metastore::VolumeType::Memory };
            Ok(RwObject::new(vol_data))
        });
    // Mock create_table to not be called, as validation should prevent it.
    mock_metastore.expect_create_table().never();


    let app = test_app(Arc::new(mock_metastore));
    let request_payload = iceberg_rest_catalog::models::CreateTableRequest {
        name: "".to_string(), // Invalid: empty table name
        location: None,
        schema: None, // This would also fail if schema is required by TableCreateRequest validation
        partition_spec: None,
        write_order: None,
        stage_create: None,
        properties: None,
    };

    let request = Request::builder()
        .uri("/v1/default/namespaces/test_db/schemas/test_schema/tables")
        .method("POST")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&request_payload).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body_bytes = to_bytes(response.into_body()).await.unwrap();
    let error_response: ErrorResponse = serde_json::from_slice(&body_bytes).unwrap();
    
    assert_eq!(error_response.error_type, "ValidationError"); // From IcebergAPIError::Validation
    assert!(error_response.message.contains("Invalid CreateTableRequest payload"));
    // The exact message from validator::ValidationErrors will be part of the source.
    // Example: "Invalid CreateTableRequest payload: name: length: value must be at least 1 character long"
    // This depends on the validation rules on core_metastore::TableCreateRequest.ident.table
    assert!(error_response.message.contains("name") || error_response.message.contains("ident")); // Check if field name part of error
}
