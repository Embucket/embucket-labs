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

    #[snafu(display("Failed to interact with connection"))]
    Interact {
        #[snafu(source)]
        error: InteractError,
        #[snafu(implicit)]
        location: Location,
    },
}