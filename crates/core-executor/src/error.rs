use common_proc::stack_trace_debug;
use datafusion_common::DataFusionError;
use df_catalog::error::Error as CatalogError;
use iceberg_rust::error::Error as IcebergError;
use iceberg_s3tables_catalog::error::Error as S3tablesError;
use snafu::Location;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[stack_trace_debug]
pub enum ExecutionError {
    // === External Library Errors (from crates outside workspace) ===
    #[snafu(display("DataFusion error: {error}"))]
    DataFusion {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion query execution failed: {error}, query: {query}"))]
    DataFusionQuery {
        #[snafu(source)]
        error: DataFusionError,
        query: String,
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

    #[snafu(display("Cannot register UDF functions: {error}"))]
    RegisterUDF {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot register UDAF functions: {error}"))]
    RegisterUDAF {
        #[snafu(source)]
        error: DataFusionError,
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

    #[snafu(display("Iceberg error: {error}"))]
    Iceberg {
        #[snafu(source)]
        error: IcebergError,
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

    #[snafu(display("UTF-8 encoding error: {error}"))]
    Utf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("URL parsing error: {error}"))]
    UrlParse {
        #[snafu(source)]
        error: url::ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    // === Internal Workspace Crate Errors ===
    #[snafu(display("Metastore error: {source}"))]
    Metastore {
        source: core_metastore::error::MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot refresh catalog list: {source}"))]
    RefreshCatalogList {
        source: CatalogError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to register catalog: {source}"))]
    RegisterCatalog {
        source: CatalogError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Threaded job error: {error}"))]
    JobError {
        #[snafu(source)]
        error: crate::dedicated_executor::JobError,
        #[snafu(implicit)]
        location: Location,
    },

    // === Internal Application Errors (no external source) ===
    #[snafu(display("Missing DataFusion session for id: {id}"))]
    MissingDataFusionSession {
        id: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Database '{db}' not found"))]
    DatabaseNotFound {
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema '{schema}' not found"))]
    SchemaNotFound {
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table '{table}' not found"))]
    TableNotFound {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Volume '{volume}' not found"))]
    VolumeNotFound {
        volume: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Catalog '{catalog}' not found"))]
    CatalogNotFound {
        catalog: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table provider not found for table: {table_name}"))]
    TableProviderNotFound {
        table_name: String,
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

    #[snafu(display("Invalid bucket identifier: {ident}"))]
    InvalidBucketIdentifier {
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

    #[snafu(display("Unsupported file format: {format}"))]
    UnsupportedFileFormat {
        format: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Object of type '{type_name}' with name '{name}' already exists"))]
    ObjectAlreadyExists {
        type_name: String,
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Catalog '{catalog}' cannot be downcasted"))]
    CatalogDownCast {
        catalog: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Catalog list downcast failed"))]
    CatalogListDowncast {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("File upload failed: {message}"))]
    UploadFailed {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Core utils error: {source}"))]
    CoreUtils {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    // User query errors
    #[snafu(display("User query error: {message}"))]
    UserQuery {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    // Not yet implemented / supported statements
    #[snafu(display("Not yet implemented / supported statement: {statement}, message: {message}"))]
    NotYetImplemented {
        statement: String,
        message: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type ExecutionResult<T> = std::result::Result<T, ExecutionError>;
