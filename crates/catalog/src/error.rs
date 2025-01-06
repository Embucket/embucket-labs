use snafu::prelude::*;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Iceberg error: {source}"))]
    Iceberg { source: iceberg::Error },

    #[snafu(display("Object store error: {source}"))]
    ObjectStore { source: object_store::Error },

    #[snafu(display("Model error: {source}"))]
    Model { source: control_plane::models::Error },

    #[snafu(display("Namespace already exists"))]
    NamespaceAlreadyExists,

    #[snafu(display("Namespace not empty"))]
    NamespaceNotEmpty,

    // TODO: These might need to have more information
    #[snafu(display("Table already exists"))]
    TableAlreadyExists,
    
    #[snafu(display("Table not found"))]
    TableNotFound,

    #[snafu(display("Database not found"))]
    DatabaseNotFound,

    #[snafu(display("Table requirement failed: {message}"))]
    TableRequirementFailed { message: String },

    #[snafu(display("Database error: {source}"))]
    Database { source: utils::Error },

    #[snafu(display("Serde error: {source}"))]
    Serde { source: serde_json::Error },
}

impl From<utils::Error> for Error {
    fn from(value: utils::Error) -> Self {
        Self::Database { source: value }
    }
}
