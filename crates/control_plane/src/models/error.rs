use snafu::prelude::*;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Invalid bucket name `{bucket_name}`. Reason: {reason}"))]
    InvalidBucketName { bucket_name: String, reason: String },

    #[snafu(display("Invalid current directory"))]
    InvalidCurrentDirectory,

    #[snafu(display("Cloud providerNot implemented"))]
    CloudProviderNotImplemented { provider: String },
}

pub type Result<T> = std::result::Result<T, Error>;