use iceberg_rust_spec::table_metadata::TableMetadataBuilderError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
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

    #[snafu(display("ObjectStore path: {source}"))]
    ObjectStorePath { source: object_store::path::Error },

    #[snafu(display(
        "Unable to create directory for File ObjectStore path {path}, error: {source}"
    ))]
    CreateDirectory {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("SlateDB error: {source}"))]
    SlateDB { source: slatedb::SlateDBError },

    #[snafu(display("SlateDB error: {source}"))]
    UtilSlateDB { source: core_utils::Error },

    #[snafu(display("Metastore object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists { type_name: String, name: String },

    #[snafu(display("Metastore object not found"))]
    ObjectNotFound,

    #[snafu(display("Volume {volume} already exists"))]
    VolumeAlreadyExists { volume: String },

    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound { volume: String },

    #[snafu(display("Database {db} already exists"))]
    DatabaseAlreadyExists { db: String },

    #[snafu(display("Database {db} not found"))]
    DatabaseNotFound { db: String },

    #[snafu(display("Schema {schema} already exists in database {db}"))]
    SchemaAlreadyExists { schema: String, db: String },

    #[snafu(display("Schema {schema} not found in database {db}"))]
    SchemaNotFound { schema: String, db: String },

    #[snafu(display("Table {table} already exists in schema {schema} in database {db}"))]
    TableAlreadyExists {
        table: String,
        schema: String,
        db: String,
    },

    #[snafu(display("Table {table} not found in schema {schema} in database {db}"))]
    TableNotFound {
        table: String,
        schema: String,
        db: String,
    },

    #[snafu(display(
        "Table Object Store for table {table} in schema {schema} in database {db} not found"
    ))]
    TableObjectStoreNotFound {
        table: String,
        schema: String,
        db: String,
    },

    #[snafu(display("Volume in use by database(s): {database}"))]
    VolumeInUse { database: String },

    #[snafu(display("Iceberg error: {source}"))]
    Iceberg { source: iceberg_rust::error::Error },

    #[snafu(display("TableMetadataBuilder error: {source}"))]
    TableMetadataBuilder { source: TableMetadataBuilderError },

    #[snafu(display("Serialization error: {source}"))]
    Serde { source: serde_json::Error },

    #[snafu(display("Validation Error: {source}"))]
    Validation { source: validator::ValidationErrors },

    #[snafu(display("UrlParse Error: {source}"))]
    UrlParse { source: url::ParseError },
}

pub type MetastoreResult<T> = std::result::Result<T, MetastoreError>;

impl From<validator::ValidationErrors> for MetastoreError {
    fn from(source: validator::ValidationErrors) -> Self {
        Self::Validation { source }
    }
}
