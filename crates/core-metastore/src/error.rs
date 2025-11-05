use error_stack_trace;
use iceberg_rust::error::Error as IcebergError;
use iceberg_rust_spec::table_metadata::TableMetadataBuilderError;
use snafu::Location;
use snafu::prelude::*;
use strum_macros::AsRefStr;
use snafu::location;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu, AsRefStr)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Failed to create directory for metastore: {error}"))]
    CreateDir {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table data already exists at that location: {path}"))]
    TableDataExists {
        path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table requirement failed: {message}"))]
    TableRequirementFailed {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Volume: Validation failed. Reason: {reason}"))]
    VolumeValidationFailed {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Volume: Missing credentials"))]
    VolumeMissingCredentials {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cloud provider not implemented"))]
    CloudProviderNotImplemented {
        provider: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ObjectStore: {error}"))]
    ObjectStore {
        #[snafu(source)]
        error: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ObjectStore path: {error}"))]
    ObjectStorePath {
        #[snafu(source)]
        error: object_store::path::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Unable to create directory for File ObjectStore path {path}, error: {error}"
    ))]
    CreateDirectory {
        path: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("SlateDB error: {error}"))]
    SlateDB {
        #[snafu(source)]
        error: slatedb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("SlateDB error: {source}"))]
    UtilSlateDB {
        #[snafu(source(from(core_utils::Error, Box::new)))]
        source: Box<core_utils::Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Metastore object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists {
        type_name: String,
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Metastore object not found"))]
    ObjectNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Volume {volume} already exists"))]
    VolumeAlreadyExists {
        volume: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound {
        volume: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Database {db} already exists"))]
    DatabaseAlreadyExists {
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Database {db} not found"))]
    DatabaseNotFound {
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema {schema} already exists in database {db}"))]
    SchemaAlreadyExists {
        schema: String,
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema {schema} not found in database {db}"))]
    SchemaNotFound {
        schema: String,
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table {table} already exists in schema {schema} in database {db}"))]
    TableAlreadyExists {
        table: String,
        schema: String,
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table {table} not found in {db}.{schema}"))]
    TableNotFound {
        table: String,
        schema: String,
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Table Object Store for table {table} in schema {schema} in database {db} not found"
    ))]
    TableObjectStoreNotFound {
        table: String,
        schema: String,
        db: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Volume in use by database(s): {database}"))]
    VolumeInUse {
        database: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Database {database} in use by schema(s): {schema}"))]
    DatabaseInUse {
        database: String,
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema {database}.{schema} in use by table(s): {table}"))]
    SchemaInUse {
        database: String,
        schema: String,
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Iceberg error: {error}"))]
    Iceberg {
        #[snafu(source(from(IcebergError, Box::new)))]
        error: Box<IcebergError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("TableMetadataBuilder error: {error}"))]
    TableMetadataBuilder {
        #[snafu(source)]
        error: TableMetadataBuilderError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Serialization error: {error}"))]
    Serde {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Validation Error: {error}"))]
    Validation {
        #[snafu(source)]
        error: validator::ValidationErrors,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("UrlParse Error: {error}"))]
    UrlParse {
        #[snafu(source)]
        error: url::ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Iceberg spec error: {error}"))]
    IcebergSpec {
        #[snafu(source)]
        error: iceberg_rust_spec::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build pool"))]
    BuildPool {
        #[snafu(source)]
        error: deadpool::managed::BuildError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error creating sqlite schema: {error}"))]
    DieselPool {
        #[snafu(source)]
        error: deadpool::managed::PoolError<deadpool_diesel::Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Core Sqlite error: {error}"))]
    CoreSqlite {
        #[snafu(source)]
        error: core_sqlite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Create metastore tables error: {error}"))]
    CreateTables {
        #[snafu(source)]
        error: rusqlite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Sql error: {error}"))]
    Sql {
        #[snafu(source)]
        error: rusqlite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Deadpool connection error: {error}"))]
    Deadpool {
        // Can't use deadpool error as it is not Send + Sync
        // as it then used by core_utils and then here: `impl From<Error> for iceberg::Error`
        #[snafu(source(from(deadpool_sqlite::InteractError, |err| core_sqlite::StringError(format!("{err:?}")))))]
        error: core_sqlite::StringError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Diesel error: {error}"))]
    Diesel {
        #[snafu(source)]
        error: diesel::result::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Generic error: {error}"))]
    Generic {
        #[snafu(source)]
        error: Box<dyn std::error::Error + 'static + Send + Sync>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("UUID parse error: {error}"))]
    UuidParse {
        #[snafu(source)]
        error: uuid::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No {name} field in RwObject: {object}"))]
    NoNamedId {
        name: String,
        object: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("RWObject id Field error: {source}"))]
    NoId {
        #[snafu(source(from(Error, Box::new)))]
        source: Box<Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("SqliteDb error: {error}"))]
    SqliteDb {
        #[snafu(source)]
        error: core_sqlite::Error,
        #[snafu(implicit)]
        location: Location,
    }
}


// One drawback using this conversion instead of .context() is about useless error location pointing to below line
impl From<deadpool_sqlite::InteractError> for Error {
    fn from(err: deadpool_sqlite::InteractError) -> Self {
        Self::Deadpool {
            error: core_sqlite::StringError(format!("{err:?}")),
            location: location!(),
        }
    }
}

// syntax sugar to use ? without .context()
impl From<deadpool::managed::PoolError<deadpool_diesel::Error>> for Error {
    fn from(error: deadpool::managed::PoolError<deadpool_diesel::Error>) -> Self {
        Self::DieselPool { error, location: location!() }
    }
}
