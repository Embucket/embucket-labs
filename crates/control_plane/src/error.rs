use iceberg_rest_catalog::models;
#[warn(dead_code)]
use quick_xml::de::from_str;
use rusoto_core::RusotoError;
use serde::Deserialize;
use snafu::prelude::*;
use datafusion::error::DataFusionError;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError},

    #[snafu(display("DataFusion query error: {source}, query: {query}"))]
    DataFusionQuery { source: DataFusionError, query: String },

    //#[snafu(display("Failed to upload data to table {warehouse_id}/{database_name}/{table_name}: {source}"))]
    //UploadData { warehouse_id: Uuid, database_name: String, table_name: String, source: DataFusionError },

    #[snafu(display("SlateDB error: {source}"))]
    SlateDB { source: utils::Error },

    #[snafu(display("IceLake error: {source}"))]
    IceLake { source: icelake::Error },

    #[snafu(display("S3 error: {source}"))]
    S3 { source: Box<dyn std::error::Error> },

    #[snafu(display("Unknown S3 error: Code: {code} Message: {message}"))]
    S3Unknown { code: String, message: String },

    #[snafu(display("Storage profile {} already in use", id))]
    StorageProfileInUse { id: Uuid },

    #[snafu(display("Invalid storage profile: {source}"))]
    InvalidStorageProfile { source: crate::models::Error },

    #[snafu(display("Unable to create Warehouse from request: {source}"))]
    InvalidCreateWarehouse { source: crate::models::Error },

    #[snafu(display("Warehouse not found: {id}"))]
    WarehouseNotFound { id: Uuid },

    #[snafu(display("Unable to delete Warehouse, not empty: {id}"))]
    WarehouseNotEmpty { id: Uuid },

    #[snafu(display("Missing storage endpoint URL"))]
    MissingStorageEndpointURL,

    #[snafu(display("Invalid storage endpoint URL: {url}, source: {source}"))]
    InvalidStorageEndpointURL { url: String, source: url::ParseError },

    // This should be refined later
    #[snafu(display("Unspported Authentication method: {method}"))]
    UnsupportedAuthenticationMethod { method: String },

    #[snafu(display("Invalid TLS configuration: {source}"))]
    InvalidTLSConfiguration { source: rusoto_core::request::TlsError },

    #[snafu(display("Catalog not found for name {name}"))]
    CatalogNotFound { name: String },

    #[snafu(display("Schema {schema} not found in database {database}"))]
    SchemaNotFound { schema: String, database: String },

    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display("Error encoding UTF8 string: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },

    #[snafu(display("Object store error: {source}"))]
    ObjectStore { source: object_store::Error },

    /*#[error("not found")]
    ErrNotFound,

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("not empty: {0}")]
    NotEmpty(String),

    #[error("invalid credentials: {0}")]
    InvalidCredentials(String),

    #[error("datafusion error: {0}")]
    DataFusionError(String),

    #[error("datafusion error: {0}")]
    IceLakeError(String),*/
}

impl From<utils::Error> for Error {
    fn from(e: utils::Error) -> Self {
        Self::SlateDB { source: e }
    }
}

impl<T: std::error::Error + Send + Sync + 'static> From<RusotoError<T>> for Error {
    fn from(e: RusotoError<T>) -> Self {
        #[derive(Snafu, Debug, Deserialize)]
        struct S3ErrorMessage {
            #[serde(rename = "Code")]
            code: String,
            #[serde(rename = "Message")]
            message: String,
        }

        match e {
            RusotoError::Unknown(ref response) => {
                let body_string = String::from_utf8_lossy(&response.body);
                if let Ok(s3_error) = from_str::<S3ErrorMessage>(&body_string.as_ref()) {
                    Error::S3Unknown { code: s3_error.code, message: s3_error.message }
                } else {
                    Error::S3Unknown { code: "unknown".to_string(), message: body_string.to_string() }
                }
            }
            _ => Error::S3 { source: Box::new(e) }
        }
    }
}

impl From<datafusion::error::DataFusionError> for Error {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        Error::DataFusion { source: err }
    }
}

impl From<icelake::Error> for Error {
    fn from(err: icelake::Error) -> Self {
        Error::IceLake { source: err }
    }
}
