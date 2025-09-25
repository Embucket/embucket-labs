use super::snowflake_error::SnowflakeError;
use core_history::QueryRecord;
use core_history::QueryRecordId;
use core_history::QueryStatus;
use datafusion_common::DataFusionError;
use df_catalog::error::Error as CatalogError;
use error_stack_trace;
use iceberg_rust::error::Error as IcebergError;
use iceberg_s3tables_catalog::error::Error as S3tablesError;
use snafu::Location;
use snafu::prelude::*;
use std::backtrace::Backtrace;
use std::fmt::Display;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Concurrency limit reached — too many concurrent queries are running"))]
    ConcurrencyLimit {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query execution exceeded timeout"))]
    QueryTimeout {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot register UDF functions"))]
    RegisterUDF {
        #[snafu(source(from(DataFusionError, Box::new)))]
        error: Box<DataFusionError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot register UDAF functions"))]
    RegisterUDAF {
        #[snafu(source(from(DataFusionError, Box::new)))]
        error: Box<DataFusionError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion error: {error}"))]
    DataFusion {
        #[snafu(source(from(DataFusionError, Box::new)))]
        error: Box<DataFusionError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected unique ownership of DiskManager"))]
    DataFusionDiskManager {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid column identifier: {ident}"))]
    InvalidColumnIdentifier {
        ident: String,
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

    #[snafu(display("Invalid database identifier: {ident}"))]
    InvalidDatabaseIdentifier {
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

    #[snafu(display("Unsupported URL scheme '{scheme}' in URL: {url}"))]
    UnsupportedUrlScheme {
        scheme: String,
        url: String,
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
        #[snafu(source(from(DataFusionError, Box::new)))]
        error: Box<DataFusionError>,
        query: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion error when building logical plan for merge target: {error}"))]
    DataFusionLogicalPlanMergeTarget {
        #[snafu(source(from(DataFusionError, Box::new)))]
        error: Box<DataFusionError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion error when building logical plan for merge source: {error}"))]
    DataFusionLogicalPlanMergeSource {
        #[snafu(source(from(DataFusionError, Box::new)))]
        error: Box<DataFusionError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion error when building logical plan for join of merge target and source: {error}"))]
    DataFusionLogicalPlanMergeJoin {
        #[snafu(source(from(DataFusionError, Box::new)))]
        error: Box<DataFusionError>,
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
        #[snafu(source(from(core_metastore::error::Error, Box::new)))]
        source: Box<core_metastore::error::Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Database {db} not found"))]
    DatabaseNotFound {
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table {table} not found in schema {schema}"))]
    TableNotFound {
        schema: String,
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table {table} not found in {db}.{schema}"))]
    TableNotFoundInSchemaInDatabase {
        operation_on: OperationOn,
        table: String,
        schema: String,
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema {schema} not found in database {db}"))]
    SchemaNotFoundInDatabase {
        operation_on: OperationOn,
        schema: String,
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound {
        volume: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Volume with type {volume_type} requires {field}"))]
    VolumeFieldRequired {
        volume_type: String,
        field: String,
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

    #[snafu(display("Object of type {type:?} with name {name} already exists"))]
    ObjectAlreadyExists {
        r#type: ObjectType,
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
        #[snafu(source(from(CatalogError, Box::new)))]
        source: Box<CatalogError>,
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
        operation_on: OperationOn,
        catalog: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("S3Tables error: {error}"))]
    S3Tables {
        #[snafu(source(from(S3tablesError, Box::new)))]
        error: Box<S3tablesError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Iceberg error: {error}"))]
    Iceberg {
        #[snafu(source(from(IcebergError, Box::new)))]
        error: Box<IcebergError>,
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
        error: tokio::task::JoinError,
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
        #[snafu(source(from(CatalogError, Box::new)))]
        source: Box<CatalogError>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to create database '{name}' without external volume"))]
    ExternalVolumeRequiredForCreateDatabase {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to drop database: {source}"))]
    DropDatabase {
        #[snafu(source(from(CatalogError, Box::new)))]
        source: Box<CatalogError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create database: {source}"))]
    CreateDatabase {
        #[snafu(source(from(CatalogError, Box::new)))]
        source: Box<CatalogError>,
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
    #[snafu(display("Only CREATE VIEW statements are supported"))]
    OnlyCreateViewStatements {
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
    #[snafu(display("{error}"))]
    UnimplementedFunction {
        #[snafu(source)]
        error: embucket_functions::visitors::unimplemented::functions_checker::UnimplementedFunctionError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("SQL parser error: {error}"))]
    SqlParser {
        #[snafu(source)]
        error: datafusion::sql::sqlparser::parser::ParserError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("NotMatchedByTarget is not supported in merge statements"))]
    NotMatchedBySourceNotSupported {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Merge inserts only support one row"))]
    MergeInsertOnlyOneRow {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("MERGE statement target must be a table"))]
    MergeTargetMustBeTable {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("MERGE statement currently supports only tables and subqueries as sources"))]
    MergeSourceNotSupported {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("MERGE statement target must be an Iceberg table"))]
    MergeTargetMustBeIcebergTable {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("LogicalPlan Extension {name} requires exactly {expected} child(ren)"))]
    LogicalExtensionChildCount {
        name: String,
        expected: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Data for not-matching file {file} is not available"))]
    MergeFilterStreamNotMatching {
        file: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Matching files have already been consumed"))]
    MatchingFilesAlreadyConsumed {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("When there are matching data files, there must be filter predicates"))]
    MissingFilterPredicates {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported IcebergValue type for literal conversion: {value_type}"))]
    UnsupportedIcebergValueType {
        value_type: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Not supported statement: {statement}"))]
    NotSupportedStatement {
        statement: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected subquery result"))]
    UnexpectedSubqueryResult {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Stages are currently not supported"))]
    StagesNotSupported {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Field '{field_name}' not found in input schema"))]
    FieldNotFoundInInputSchema {
        field_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Can't cast to {v}"))]
    CantCastTo {
        v: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{}: Query execution error: {source}", query_id.as_uuid()))]
    QueryExecution {
        query_id: QueryRecordId,
        #[snafu(source(from(Error, Box::new)))]
        source: Box<Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query {} isn't running", query_id.as_uuid()))]
    QueryIsntRunning {
        query_id: QueryRecordId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query with request_id {request_id} isn't running"))]
    QueryByRequestIdIsntRunning {
        request_id: uuid::Uuid,
        #[snafu(implicit)]
        location: Location,
    },

    // When user tried to get result before query finished
    #[snafu(display("Query {} is running", query_id.as_uuid()))]
    QueryIsRunning {
        query_id: QueryRecordId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query History error: {source}"))]
    QueryHistory {
        #[snafu(source(from(core_history::errors::Error, Box::new)))]
        source: Box<core_history::errors::Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query {query_id} cancelled"))]
    QueryCancelled {
        query_id: QueryRecordId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query [{query_id}] result sending error"))]
    QueryResultSend {
        query_id: QueryRecordId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query [{query_id}] result recv error: {error}"))]
    QueryResultRecv {
        query_id: QueryRecordId,
        #[snafu(source)]
        error: tokio::sync::oneshot::error::RecvError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query [{query_id}] result notify error: {error}"))]
    QueryStatusRecv {
        query_id: QueryRecordId,
        #[snafu(source)]
        error: tokio::sync::watch::error::RecvError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query [{query_id}] status notify error: {error}"))]
    NotifyQueryStatus {
        query_id: QueryRecordId,
        #[snafu(source)]
        error: tokio::sync::watch::error::SendError<QueryStatus>,
        #[snafu(implicit)]
        location: Location,
    },

    // This is logical error, means error getting error from QueryRecord as it contains result data
    #[snafu(display(""))]
    HistoricalQueryContainsData {
        #[snafu(implicit)]
        location: Location,
    },

    // Just a text error loaded from QueryHistory
    #[snafu(display("{error}"))]
    HistoricalQueryError {
        error: String,
    },

    #[snafu(display("Failed to bootstrap {entity_type}: {source}"))]
    Bootstrap {
        entity_type: String,
        #[snafu(source(from(core_metastore::error::Error, Box::new)))]
        source: Box<core_metastore::error::Error>,
        #[snafu(implicit)]
        location: Location,
    },
}

impl Error {
    pub fn query_id(&self) -> QueryRecordId {
        if let Self::QueryExecution { query_id, .. } = self {
            *query_id
        } else {
            QueryRecordId::default()
        }
    }
    #[must_use]
    pub fn to_snowflake_error(&self) -> SnowflakeError {
        SnowflakeError::from_executor_error(self)
    }
    // These looks usefull, though not used anymore: is_query_cancelled, is_query_timeout
    #[must_use]
    pub const fn is_query_cancelled(&self) -> bool {
        if let Self::QueryExecution { source, .. } = self {
            source.is_query_cancelled()
        } else {
            matches!(self, Self::QueryCancelled { .. })
        }
    }
    #[must_use]
    pub const fn is_query_timeout(&self) -> bool {
        if let Self::QueryExecution { source, .. } = self {
            source.is_query_timeout()
        } else {
            matches!(self, Self::QueryTimeout { .. })
        }
    }
}

impl TryFrom<QueryRecord> for Error {
    type Error = Self;
    fn try_from(value: QueryRecord) -> std::result::Result<Self, Self::Error> {
        value.error.map_or_else(
            || Err(HistoricalQueryContainsDataSnafu {}.build()),
            |error| Ok(HistoricalQuerySnafu { error }.build()),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    Create,
    Alter,
    Drop,
    Show,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationOn {
    Database(OperationType),
    Schema(OperationType),
    Table(OperationType),
    Unknown,
}

#[derive(Debug)]
pub enum ObjectType {
    Volume,
    Database,
    Schema,
    Table,
}

impl Display for ObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Volume => write!(f, "volume"),
            Self::Database => write!(f, "database"),
            Self::Schema => write!(f, "schema"),
            Self::Table => write!(f, "table"),
        }
    }
}

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(value))
    }
}
