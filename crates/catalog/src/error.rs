use snafu::prelude::*;
pub type CatalogResult<T> = Result<T, CatalogError>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum CatalogError {
    #[snafu(display("Iceberg error: {source}"))]
    Iceberg { source: iceberg::Error },

    #[snafu(display("Object store error: {source}"))]
    ObjectStore { source: object_store::Error },

    #[snafu(display("Control plane error: {source}"))]
    ControlPlane {
        source: control_plane::models::ControlPlaneModelError,
    },

    #[snafu(display("Namespace already exists: {key}"))]
    NamespaceAlreadyExists { key: String },

    #[snafu(display("Namespace not empty: {key}"))]
    NamespaceNotEmpty { key: String },

    #[snafu(display("Table already exists: {key}"))]
    TableAlreadyExists { key: String },

    #[snafu(display("Table not found: {key}"))]
    TableNotFound { key: String },

    #[snafu(display("Database not found: {key}"))]
    DatabaseNotFound { key: String },

    #[snafu(display("Table requirement failed: {message}"))]
    TableRequirementFailed { message: String },

    #[snafu(display("State store error: {source}"))]
    StateStore { source: utils::Error },

    #[snafu(display("Serde error: {source}"))]
    Serde { source: serde_json::Error },
}

impl From<utils::Error> for CatalogError {
    fn from(value: utils::Error) -> Self {
        Self::StateStore { source: value }
    }
}
