use snafu::prelude::*;

// Errors that are specific to the `models` crate
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Invalid bucket name `{bucket_name}`. Reason: {reason}"))]
    InvalidBucketName { bucket_name: String, reason: String },

    #[snafu(display("Invalid directory `{directory}`"))]
    InvalidDirectory { directory: String },

    #[snafu(display("Cloud providerNot implemented"))]
    CloudProviderNotImplemented { provider: String },

    #[snafu(display("Unable to parse key `{key}`"))]
    UnableToParseConfiguration { key: String, source: Box<dyn std::error::Error> },

    #[snafu(display("Role-based credentials aren't supported"))]
    RoleBasedCredentialsNotSupported,
}

pub type Result<T> = std::result::Result<T, Error>;