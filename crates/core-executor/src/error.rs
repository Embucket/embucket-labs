use std::backtrace::Backtrace;

use datafusion_common::DataFusionError;
use df_catalog::error::Error as CatalogError;
use iceberg_rust::error::Error as IcebergError;
use iceberg_s3tables_catalog::error::Error as S3tablesError;
use snafu::Location;
use snafu::prelude::*;
use stack_error_proc::stack_trace_debug;

pub type ExecutionResult<T> = std::result::Result<T, ExecutionError>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum ExecutionError {
    #[snafu(display("Cannot register UDF functions"))]
    RegisterUDF {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot register UDAF functions"))]
    RegisterUDAF {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion error: {error}"))]
    DataFusion {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid table identifier: {ident}"))]
    InvalidTableIdentifier {
        ident: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid schema identifier: {ident}"))]
    InvalidSchemaIdentifier {
        ident: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid file path: {path}"))]
    InvalidFilePath {
        path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid bucket identifier: {ident}"))]
    InvalidBucketIdentifier {
        ident: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Arrow error: {error}"))]
    Arrow {
        #[snafu(source)]
        error: datafusion::arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No Table Provider found for table: {table_name}"))]
    TableProviderNotFound {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing DataFusion session for id {id}"))]
    MissingDataFusionSession {
        id: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion query error: {error}, query: {query}"))]
    DataFusionQuery {
        #[snafu(source)]
        error: DataFusionError,
        query: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error encoding UTF8 string: {error}"))]
    Utf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Metastore error: {source}"))]
    Metastore {
        source: core_metastore::error::MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Database {db} not found"))]
    DatabaseNotFound {
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table {table} not found"))]
    TableNotFound {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema {schema} not found"))]
    SchemaNotFound {
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound {
        volume: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Object store error: {error}"))]
    ObjectStore {
        #[snafu(source)]
        error: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists {
        type_name: String,
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported file format {format}"))]
    UnsupportedFileFormat {
        format: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot refresh catalog list: {source}"))]
    RefreshCatalogList {
        source: CatalogError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Catalog {catalog} cannot be downcasted"))]
    CatalogDownCast {
        catalog: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Catalog {catalog} not found"))]
    CatalogNotFound {
        catalog: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("S3Tables error: {error}"))]
    S3Tables {
        #[snafu(source)]
        error: S3tablesError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Iceberg error: {error}"))]
    Iceberg {
        #[snafu(source)]
        error: IcebergError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("URL Parsing error: {error}"))]
    UrlParse {
        #[snafu(source)]
        error: url::ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Threaded Job error: {error}: {backtrace}"))]
    JobError {
        #[snafu(source)]
        error: crate::dedicated_executor::JobError,
        backtrace: Backtrace,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to upload file: {message}"))]
    UploadFailed {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("CatalogList failed"))]
    CatalogListDowncast {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to register catalog: {source}"))]
    RegisterCatalog {
        #[snafu(source)]
        source: CatalogError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse data: {error}"))]
    SerdeParse {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Only USE with variables are supported"))]
    OnyUseWithVariables {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only primitive statements are supported"))]
    OnlyPrimitiveStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only CREATE TABLE/CREATE SCHEMA statements are supported"))]
    OnlyTableSchemaCreateStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only DROP statements are supported"))]
    OnlyDropStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only DROP TABLE/VIEW statements are supported"))]
    OnlyDropTableViewStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only CREATE TABLE statements are supported"))]
    OnlyCreateTableStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only CREATE STAGE statements are supported"))]
    OnlyCreateStageStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only COPY INTO statements are supported"))]
    OnlyCopyIntoStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("FROM object is required for COPY INTO statements"))]
    FromObjectRequiredForCopyIntoStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only MERGE statements are supported"))]
    OnlyMergeStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only CREATE SCHEMA statements are supported"))]
    OnlyCreateSchemaStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only simple schema names are supported"))]
    OnlySimpleSchemaNames {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("unsupported SHOW statement: {statement}"))]
    UnsupportedShowStatement {
        statement: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No table names provided for TRUNCATE TABLE"))]
    NoTableNamesForTruncateTable {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Only SQL statements are supported"))]
    OnlySQLStatements {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Missing or invalid column: '{name}'"))]
    MissingOrInvalidColumn {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
}
