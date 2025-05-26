use std::{collections::HashMap, sync::Arc, backtrace::Backtrace};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::{
    array::RecordBatch,
    csv::reader::Format,
    datatypes::{DataType, Field, TimeUnit},
    error::ArrowError as DataFusionArrowError, // Renamed to avoid conflict if ArrowError is used directly
};
use datafusion_common::{DataFusionError};
use iceberg_rust::error::Error as IcebergError;
use iceberg_s3tables_catalog::error::Error as S3tablesError;
use object_store::Error as ObjectStoreError;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use url::ParseError as UrlParseError;

// Forward declaration for items that will be in other modules of this crate (core-traits)
// For now, we assume MetastoreTableIdent will be available from a `metastore` mod in this crate
// and UserSession, QueryContext, Config from this crate's modules or they will be generic.
// This is a placeholder and will need actual paths when other modules are created or items are made generic.
// For now, we'll use placeholder types/paths to get the file written.
// These will likely cause errors later, guiding the next steps.
use crate::metastore::TableIdent as MetastoreTableIdent; // Corrected path
// Placeholder for types that were previously in core_executor. These need to be defined or made generic.
// For the initial move, we define them here or expect them to be generic parameters on the trait.
// Since UserSession, QueryContext, Config are complex types from core_executor,
// they ideally should become generic parameters or be moved as well if they are pure data.
// For now, let's assume they will be defined elsewhere or made generic.
// To make the code somewhat parsable, I'll add dummy struct definitions for now.
// THIS IS A TEMPORARY MEASURE for this step.
pub struct UserSession; // Placeholder
pub struct QueryContext; // Placeholder
pub struct Config; // Placeholder
pub mod dedicated_executor { // Placeholder for JobError source
    #[derive(Debug, Snafu)]
    pub enum JobError {
        // Placeholder variant
        SomeError,
    }
}
pub mod error { // Placeholder for core_metastore::error
    #[derive(Debug, Snafu)]
    pub enum MetastoreError {
        // Placeholder variant
        SomeError,
    }
}
pub mod df_catalog_error { // Placeholder for df_catalog::error
    #[derive(Debug, Snafu)]
    pub enum Error {
        // Placeholder variant
        SomeError,
    }
}


// --- ExecutionError and ExecutionResult ---
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))] // Changed to pub
pub enum ExecutionError {
    #[snafu(display("Cannot register UDF functions"))]
    RegisterUDF { source: DataFusionError },

    #[snafu(display("Cannot register UDAF functions"))]
    RegisterUDAF { source: DataFusionError },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError },

    #[snafu(display("Invalid table identifier: {ident}"))]
    InvalidTableIdentifier { ident: String },

    #[snafu(display("Invalid schema identifier: {ident}"))]
    InvalidSchemaIdentifier { ident: String },

    #[snafu(display("Invalid file path: {path}"))]
    InvalidFilePath { path: String },

    #[snafu(display("Invalid bucket identifier: {ident}"))]
    InvalidBucketIdentifier { ident: String },

    #[snafu(display("Arrow error: {source}"))]
    Arrow {
        source: DataFusionArrowError,
    },

    #[snafu(display("No Table Provider found for table: {table_name}"))]
    TableProviderNotFound { table_name: String },

    #[snafu(display("Missing DataFusion session for id {id}"))]
    MissingDataFusionSession { id: String },

    #[snafu(display("DataFusion query error: {source}, query: {query}"))]
    DataFusionQuery {
        source: DataFusionError,
        query: String,
    },

    #[snafu(display("Error encoding UTF8 string: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },

    #[snafu(display("Metastore error: {source}"))]
    Metastore {
        source: crate::metastore::MetastoreError, // Adjusted path to the new location
    },

    #[snafu(display("Database {db} not found"))]
    DatabaseNotFound { db: String },

    #[snafu(display("Table {table} not found"))]
    TableNotFound { table: String },

    #[snafu(display("Schema {schema} not found"))]
    SchemaNotFound { schema: String },

    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound { volume: String },

    #[snafu(display("Object store error: {source}"))]
    ObjectStore { source: ObjectStoreError },

    #[snafu(display("Object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists { type_name: String, name: String },

    #[snafu(display("Unsupported file format {format}"))]
    UnsupportedFileFormat { format: String },

    #[snafu(display("Cannot refresh catalog list: {source}"))]
    RefreshCatalogList { source: Box<dyn std::error::Error + Send + Sync + 'static> }, // Changed to Boxed Error

    #[snafu(display("Catalog {catalog} cannot be downcasted"))]
    CatalogDownCast { catalog: String },

    #[snafu(display("Catalog {catalog} not found"))]
    CatalogNotFound { catalog: String },

    #[snafu(display("S3Tables error: {source}"))]
    S3Tables { source: S3tablesError },

    #[snafu(display("Iceberg error: {source}"))]
    Iceberg { source: IcebergError },

    #[snafu(display("URL Parsing error: {source}"))]
    UrlParse { source: UrlParseError },

    #[snafu(display("Threaded Job error: {source}: {backtrace}"))]
    JobError {
        source: Box<dyn std::error::Error + Send + Sync + 'static>, // Changed to Boxed Error
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to upload file: {message}"))]
    UploadFailed { message: String },

    #[snafu(display("CatalogList failed"))]
    CatalogListDowncast,

    #[snafu(display("Failed to register catalog: {source}"))]
    RegisterCatalog { source: Box<dyn std::error::Error + Send + Sync + 'static> }, // Changed to Boxed Error
}

pub type ExecutionResult<T> = std::result::Result<T, ExecutionError>;

// --- QueryResultData and ColumnInfo ---
#[derive(Debug)]
pub struct QueryResultData {
    pub records: Vec<RecordBatch>,
    pub columns_info: Vec<ColumnInfo>,
    pub query_id: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub nullable: bool,
    pub r#type: String,
    pub byte_length: Option<i32>,
    pub length: Option<i32>,
    pub scale: Option<i32>,
    pub precision: Option<i32>,
    pub collation: Option<String>,
}

impl ColumnInfo {
    #[must_use]
    pub fn to_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("logicalType".to_string(), self.r#type.to_uppercase());
        metadata.insert(
            "precision".to_string(),
            self.precision.unwrap_or(38).to_string(),
        );
        metadata.insert("scale".to_string(), self.scale.unwrap_or(0).to_string());
        metadata.insert(
            "charLength".to_string(),
            self.length.unwrap_or(0).to_string(),
        );
        metadata
    }

    #[must_use]
    pub fn from_batch(records: &[RecordBatch]) -> Vec<Self> {
        let mut column_infos = Vec::new();

        if records.is_empty() {
            return column_infos;
        }
        for field in records[0].schema().fields() {
            column_infos.push(Self::from_field(field));
        }
        column_infos
    }

    #[must_use]
    pub fn from_field(field: &Field) -> Self {
        let mut column_info = Self {
            name: field.name().clone(),
            database: String::new(),
            schema: String::new(),
            table: String::new(),
            nullable: field.is_nullable(),
            r#type: field.data_type().to_string(),
            byte_length: None,
            length: None,
            scale: None,
            precision: None,
            collation: None,
        };

        match field.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                column_info.r#type = "fixed".to_string();
                column_info.precision = Some(38);
                column_info.scale = Some(0);
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                column_info.r#type = "real".to_string();
                column_info.precision = Some(38);
                column_info.scale = Some(16);
            }
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                column_info.r#type = "fixed".to_string();
                column_info.precision = Some(i32::from(*precision));
                column_info.scale = Some(i32::from(*scale));
            }
            DataType::Boolean => {
                column_info.r#type = "boolean".to_string();
            }
            DataType::Utf8 => {
                column_info.r#type = "text".to_string();
                column_info.byte_length = Some(16_777_216);
                column_info.length = Some(16_777_216);
            }
            DataType::Time32(_) | DataType::Time64(_) => {
                column_info.r#type = "time".to_string();
                column_info.precision = Some(0);
                column_info.scale = Some(9);
            }
            DataType::Date32 | DataType::Date64 => {
                column_info.r#type = "date".to_string();
            }
            DataType::Timestamp(unit, _) => {
                column_info.r#type = "timestamp_ntz".to_string();
                column_info.precision = Some(0);
                let scale = match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                };
                column_info.scale = Some(scale);
            }
            DataType::Binary | DataType::BinaryView => {
                column_info.r#type = "binary".to_string();
                column_info.byte_length = Some(8_388_608);
                column_info.length = Some(8_388_608);
            }
            _ => {
                column_info.r#type = "text".to_string();
            }
        }
        column_info
    }
}

// --- ExecutionService Trait ---
#[async_trait]
pub trait ExecutionService: Send + Sync {
    async fn create_session(&self, session_id: String) -> ExecutionResult<Arc<UserSession>>;
    async fn delete_session(&self, session_id: String) -> ExecutionResult<()>;
    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> ExecutionResult<QueryResultData>;
    async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &MetastoreTableIdent, // Assuming this path is correct for future metastore trait
        data: Bytes,
        file_name: &str,
        format: Format,
    ) -> ExecutionResult<usize>;
    fn config(&self) -> &Config;
}


// --- Tests for ColumnInfo ---
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::TimeUnit; // Already in use statements above

    // This will require tokio as a dev-dependency for core-traits
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_column_info_from_field() {
        let field = Field::new("test_field", DataType::Int8, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "fixed");
        assert!(!column_info.nullable);

        let field = Field::new("test_field", DataType::Decimal128(1, 2), true);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "fixed");
        assert_eq!(column_info.precision.unwrap(), 1);
        assert_eq!(column_info.scale.unwrap(), 2);
        assert!(column_info.nullable);

        let field = Field::new("test_field", DataType::Boolean, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "boolean");

        let field = Field::new("test_field", DataType::Time32(TimeUnit::Second), false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "time");
        assert_eq!(column_info.precision.unwrap(), 0);
        assert_eq!(column_info.scale.unwrap(), 9);

        let field = Field::new("test_field", DataType::Date32, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "date");

        let units = [
            (TimeUnit::Second, 0),
            (TimeUnit::Millisecond, 3),
            (TimeUnit::Microsecond, 6),
            (TimeUnit::Nanosecond, 9),
        ];
        for (unit, scale) in units {
            let field = Field::new("test_field", DataType::Timestamp(unit, None), false);
            let column_info = ColumnInfo::from_field(&field);
            assert_eq!(column_info.name, "test_field");
            assert_eq!(column_info.r#type, "timestamp_ntz");
            assert_eq!(column_info.precision.unwrap(), 0);
            assert_eq!(column_info.scale.unwrap(), scale);
        }

        let field = Field::new("test_field", DataType::Binary, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "binary");
        assert_eq!(column_info.byte_length.unwrap(), 8_388_608);
        assert_eq!(column_info.length.unwrap(), 8_388_608);

        let field = Field::new("test_field", DataType::Utf8View, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "text");
        assert_eq!(column_info.byte_length, None);
        assert_eq!(column_info.length, None);

        let floats = [
            (DataType::Float16, 16, true),
            (DataType::Float32, 16, true),
            (DataType::Float64, 16, true),
            (DataType::Float64, 17, false),
        ];
        for (float_datatype, scale, outcome) in floats {
            let field = Field::new("test_field", float_datatype, false);
            let column_info = ColumnInfo::from_field(&field);
            assert_eq!(column_info.name, "test_field");
            assert_eq!(column_info.r#type, "real");
            assert_eq!(column_info.precision.unwrap(), 38);
            if outcome {
                assert_eq!(column_info.scale.unwrap(), scale);
            } else {
                assert_ne!(column_info.scale.unwrap(), scale);
            }
        }
    }

    #[tokio::test]
    async fn test_to_metadata() {
        let column_info = ColumnInfo {
            name: "test_field".to_string(),
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            nullable: false,
            r#type: "fixed".to_string(),
            byte_length: Some(8_388_608),
            length: Some(8_388_608),
            scale: Some(0),
            precision: Some(38),
            collation: None,
        };
        let metadata = column_info.to_metadata();
        assert_eq!(metadata.get("logicalType"), Some(&"FIXED".to_string()));
        assert_eq!(metadata.get("precision"), Some(&"38".to_string()));
        assert_eq!(metadata.get("scale"), Some(&"0".to_string()));
        assert_eq!(metadata.get("charLength"), Some(&"8388608".to_string()));
    }
}
