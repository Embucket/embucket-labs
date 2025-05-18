use snafu::prelude::*;
use super::http::HttpRequestError;

pub type AuthenticatedQueryResult<T> = std::result::Result<T, QueryRequestError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QueryRequestError {
    #[snafu(display("Query error: {source}"))]
    QueryRequest { source: HttpRequestError },
}
