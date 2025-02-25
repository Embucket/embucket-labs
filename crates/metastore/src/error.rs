use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum MetastoreError {
    #[snafu(display("Table data already exists at that location: {location}"))]
    TableDataExists { location: String },

    #[snafu(display("Table requirement failed: {message}"))]
    TableRequirementFailed { message: String },

    #[snafu(display("Volume: Validation failed. Reason: {reason}"))]
    VolumeValidationFailed { reason: String },

    #[snafu(display("Volume: Missing credentials"))]
    VolumeMissingCredentials,

    #[snafu(display("Cloud provider not implemented"))]
    CloudProviderNotImplemented { provider: String },

    #[snafu(display("ObjectStore: {source}"))]
    ObjectStore { source: object_store::Error },

    #[snafu(display("Unable to create directory for File ObjectStore path {path}, error: {source}"))]
    CreateDirectory { path: String, source: std::io::Error },

    #[snafu(display("SlateDB error: {source}"))]
    SlateDB { source: slatedb::error::SlateDBError },

    #[snafu(display("SlateDB error: {source}"))]
    UtilSlateDB { source: utils::Error },

    #[snafu(display("Metastore object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists { type_name: String, name: String },

    #[snafu(display("Metastore object of type {type_name} with name {name} not found"))]
    ObjectNotFound { type_name: String, name: String },

    #[snafu(display("Volume in use by database(s): {database}"))]
    VolumeInUse { database: String },

    #[snafu(display("Iceberg error: {source}"))]
    Iceberg { source: iceberg::Error },

    #[snafu(display("Seriliazation error: {source}"))]
    Serde { source: serde_json::Error },
}

pub type MetastoreResult<T> = std::result::Result<T, MetastoreError>;