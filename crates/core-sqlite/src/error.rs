use snafu::Location;
use snafu::Snafu;

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

    #[snafu(display("Failed to lock connections"))]
    ConnectionsLock {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("R2d2 error {error}"))]
    R2d2 {
        #[snafu(source)]
        error: r2d2::Error,
        #[snafu(implicit)]
        location: Location,
    }
}