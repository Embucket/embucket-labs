use snafu::prelude::*;

// Errors that are specific to the `models` crate
#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum ControlPlaneModelError {
    #[snafu(display("Invalid bucket name `{bucket_name}`. Reason: {reason}"))]
    InvalidBucketName { bucket_name: String, reason: String },

    #[snafu(display("Invalid region name `{region}`. Reason: {reason}"))]
    InvalidRegionName { region: String, reason: String },

    #[snafu(display("Invalid directory `{directory}`"))]
    InvalidDirectory { directory: String },

    #[snafu(display("Invalid endpoint url `{url}`"))]
    InvalidEndpointUrl {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display("Cloud providerNot implemented"))]
    CloudProviderNotImplemented { provider: String },

    #[snafu(display("Unable to parse key `{key}`"))]
    UnableToParseConfiguration {
        key: String,
        source: Box<dyn std::error::Error + Send>,
    },

    #[snafu(display("Role-based credentials aren't supported"))]
    RoleBasedCredentialsNotSupported,

    #[snafu(display("Missing credentials for {profile_type} profile type"))]
    MissingCredentials { profile_type: String },

    #[snafu(display("Missing environment variable `{var}`"))]
    MissingEnvironmentVariable {
        source: std::env::VarError,
        var: String,
    },

    // Duplicated in `control_plane` crate, needs refactoring
    #[snafu(display("Object store error: {source}"))]
    ObjectStore { source: object_store::Error },
}

pub type ControlPlaneModelResult<T> = std::result::Result<T, ControlPlaneModelError>;
