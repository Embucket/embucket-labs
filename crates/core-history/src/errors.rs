use crate::QueryRecordId;
use error_stack_trace;
use snafu::Location;
use snafu::Snafu;
use snafu::location;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Error using key: {error}"))]
    BadKey {
        #[snafu(source)]
        error: std::str::Utf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error adding worksheet: {source}"))]
    WorksheetAdd {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error getting worksheet: {source}"))]
    WorksheetGet {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error getting worksheets: {source}"))]
    WorksheetsList {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error deleting worksheet: {source}"))]
    WorksheetDelete {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error updating worksheet: {source}"))]
    WorksheetUpdate {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error adding query record: {source}"))]
    QueryAdd {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error updating query record: {source}"))]
    QueryUpdate {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Can't locate query record by query_id: {} ({})", query_id.as_uuid(), query_id.as_i64()))]
    QueryNotFound {
        query_id: QueryRecordId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error adding query record reference: {source}"))]
    QueryReferenceAdd {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error getting query history: {source}"))]
    QueryGet {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Can't locate worksheet by key: {message}"))]
    WorksheetNotFound {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Bad query record reference key: {key}"))]
    QueryReferenceKey {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error getting worksheet queries: {source}"))]
    GetWorksheetQueries {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error adding query inverted key: {source}"))]
    QueryInvertedKeyAdd {
        source: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query item seek error: {error}"))]
    Seek {
        #[snafu(source)]
        error: slatedb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Deserialize error: {error}"))]
    DeserializeValue {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query execution error: {message}"))]
    ExecutionResult {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No result set for QueryRecord: {}", query_id.as_uuid()))]
    NoResultSet {
        query_id: QueryRecordId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("CoreUtils Sqlite error: {error}"))]
    CoreUtils {
        #[snafu(source)]
        error: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Sqlite error: {error}"))]
    RuSqlite {
        #[snafu(source)]
        error: rusqlite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Create tables error: {error}"))]
    CreateTables {
        #[snafu(source)]
        error: rusqlite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Deadpool connection error: {error}"))]
    Deadpool {
        // Can't use deadpool error as it is not Send + Sync
        // as it then useing by core_utils and then here: `impl From<Error> for iceberg::Error`
        #[snafu(source(from(deadpool_sqlite::InteractError, |err| core_sqlite::StringError(format!("{:?}", err)))))]
        error: core_sqlite::StringError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<deadpool_sqlite::InteractError> for Error {
    fn from(err: deadpool_sqlite::InteractError) -> Self {
        Error::Deadpool { error: core_sqlite::StringError(format!("{:?}", err)), location: location!() }
    }
}