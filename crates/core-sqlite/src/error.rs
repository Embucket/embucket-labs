use deadpool_sqlite::InteractError;
use deadpool_sqlite::{CreatePoolError, PoolError};
use snafu::Location;
use snafu::{Snafu, location};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Rusqlite error {error}"))]
    Rusqlite {
        #[snafu(source)]
        error: rusqlite::Error,
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
        // as it then used by core_utils and then here: `impl From<Error> for iceberg::Error`
        #[snafu(source(from(InteractError, |err| StringError(format!("{err:?}")))))]
        error: StringError,
        #[snafu(implicit)]
        location: Location,
    },
}

// to make `?` work instead of `.context(DeadpoolSnafu)`
impl From<InteractError> for Error {
    fn from(err: InteractError) -> Self {
        Self::Deadpool {
            error: StringError(format!("{err:?}")),
            location: location!(),
        }
    }
}

// to make `?` work instead of `.context(RusqliteSnafu)`
// Note: when using ? instead of .context(), it uses artifical error location
impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::Rusqlite {
            error: err,
            location: location!(),
        }
    }
}

#[derive(Debug)]
pub struct StringError(pub String);
impl std::fmt::Display for StringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for StringError {}
