use crate::error::IntoStatusCode;
use http::Error as HttpError;
use http::StatusCode;
use snafu::Location;
use snafu::Snafu;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("File not found: {path}"))]
    NotFound {
        path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Response body error: {error}"))]
    ResponseBody {
        #[snafu(source)]
        error: HttpError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Bad archive: {error}"))]
    BadArchive {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Entry path is not a valid Unicode: {error}"))]
    NonUnicodeEntryPathInArchive {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Entry data read error: {error}"))]
    ReadEntryData {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::BadArchive { .. } | Self::ResponseBody { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::NonUnicodeEntryPathInArchive { .. } | Self::ReadEntryData { .. } => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
        }
    }
}
