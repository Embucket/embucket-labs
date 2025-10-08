use deadpool_sqlite::InteractError;
use snafu::Location;
use snafu::Snafu;
use deadpool_sqlite::{PoolError, CreatePoolError};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Sqlite not initialized yet"))]
    SqliteNotInitializedYet {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to initialize sqlite store"))]
    FailedToInitializeSqliteStore{
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Rusqlite error {error}"))]
    Rusqlite {
        #[snafu(source)]
        error: rusqlite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No VFS detected"))]
    NoVfsDetected {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Sqlite self check failed"))]
    SelfCheck {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to lock connections"))]
    ConnectionsLock {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Deadpool error {error}"))]
    Pool {
        #[snafu(source)]
        error: PoolError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create pool"))]
    CreatePool {
        #[snafu(source)]
        error: CreatePoolError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Deadpool connection error: {error}"))]
    Deadpool {
        // Can't use deadpool error as it is not Send + Sync
        // as it then useing by core_utils and then here: `impl From<Error> for iceberg::Error`
        #[snafu(source(from(InteractError, |err| StringError(format!("{:?}", err)))))]
        error: StringError,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug)]
pub struct StringError(pub String);
impl std::fmt::Display for StringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for StringError {}