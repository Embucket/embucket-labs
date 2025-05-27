use std::backtrace::Backtrace;

use datafusion_common::DataFusionError;
use df_catalog::error::Error as CatalogError;
use iceberg_rust::error::Error as IcebergError;
use iceberg_s3tables_catalog::error::Error as S3tablesError;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
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
        source: datafusion::arrow::error::ArrowError,
    },

    #[snafu(display("No Table Provider found for table: {table_name}"))]
    TableProviderNotFound { table_name: String },

    #[snafu(display("Missing DataFusion session for id {id}"))]
    MissingDataFusionSession { id: String },

    #[snafu(display("DataFusion query error: {source}, query: {query}"))]
    DataFusionQuery {
        #[snafu(source(from(DataFusionError, Box::new)))]
        source: Box<DataFusionError>,
        query: String,
    },

    #[snafu(display("Error encoding UTF8 string: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },

    #[snafu(display("Metastore error: {source}"))]
    Metastore {
        source: Box<core_metastore::error::MetastoreError>,
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
    ObjectStore { source: object_store::Error },

    #[snafu(display("Object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists { type_name: String, name: String },

    #[snafu(display("Unsupported file format {format}"))]
    UnsupportedFileFormat { format: String },

    #[snafu(display("Cannot refresh catalog list: {source}"))]
    RefreshCatalogList { source: CatalogError },

    #[snafu(display("Catalog {catalog} cannot be downcasted"))]
    CatalogDownCast { catalog: String },

    #[snafu(display("Catalog {catalog} not found"))]
    CatalogNotFound { catalog: String },

    #[snafu(display("S3Tables error: {source}"))]
    S3Tables {
        #[snafu(source(from(S3tablesError, Box::new)))]
        source: Box<S3tablesError>,
    },

    #[snafu(display("Iceberg error: {source}"))]
    Iceberg {
        #[snafu(source(from(IcebergError, Box::new)))]
        source: Box<IcebergError>,
    },

    #[snafu(display("URL Parsing error: {source}"))]
    UrlParse { source: url::ParseError },

    #[snafu(display("Threaded Job error: {source}: {backtrace}"))]
    JobError {
        source: crate::dedicated_executor::JobError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to upload file: {message}"))]
    UploadFailed { message: String },

    #[snafu(display("CatalogList failed"))]
    CatalogListDowncast,

    #[snafu(display("Failed to register catalog: {source}"))]
    RegisterCatalog { source: CatalogError },
}

pub type ExecutionResult<T> = std::result::Result<T, ExecutionError>;
