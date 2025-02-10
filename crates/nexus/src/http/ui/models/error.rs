use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use catalog::error::CatalogError;
use catalog::models::{DatabaseIdent, TableIdent};
use control_plane::error::ControlPlaneError;
use snafu::prelude::*;
use utoipa::{PartialSchema, ToSchema};
use uuid::Uuid;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum NexusError {
    #[snafu(display("Failed to fetch warehouse with id {id}"))]
    WarehouseFetch { id: Uuid, source: ControlPlaneError },

    #[snafu(display("Failed to fetch storage profile with id {id}"))]
    StorageProfileFetch { id: Uuid, source: ControlPlaneError },

    #[snafu(display("Failed to fetch database with id {id}"))]
    DatabaseFetch {
        id: DatabaseIdent,
        source: CatalogError,
    },

    #[snafu(display("Failed to list warehouses"))]
    WarehouseList { source: ControlPlaneError },

    #[snafu(display("Failed to list database models for warehouse with id {id}"))]
    DatabaseModelList { id: Uuid, source: CatalogError },

    #[snafu(display("Failed to list namespaces for warehouse with id {id}"))]
    NamespaceList { id: Uuid, source: CatalogError },

    #[snafu(display("Failed to list tables for database with id {id}"))]
    TableList {
        id: DatabaseIdent,
        source: CatalogError,
    },

    #[snafu(display("Failed to create table"))]
    TableCreate { source: CatalogError },

    #[snafu(display("Failed to fetch table with id {id}"))]
    TableFetch {
        id: TableIdent,
        source: CatalogError,
    },

    #[snafu(display("Failed to delete table"))]
    TableDelete { source: CatalogError },

    #[snafu(display("Failed to register table"))]
    TableRegister { source: CatalogError },

    #[snafu(display("Database with name {name} already exists"))]
    DatabaseAlreadyExists { name: String },

    #[snafu(display("Failed to create database with ident {ident}"))]
    DatabaseCreate {
        ident: DatabaseIdent,
        source: CatalogError,
    },

    #[snafu(display("Failed to delete database with ident {ident}"))]
    DatabaseDelete {
        ident: DatabaseIdent,
        source: CatalogError,
    },

    #[snafu(display("Failed to create storage profile"))]
    StorageProfileCreate { source: ControlPlaneError },

    #[snafu(display("Failed to delete storage profile with id {id}"))]
    StorageProfileDelete { id: Uuid, source: ControlPlaneError },

    #[snafu(display("Failed to list storage profiles"))]
    StorageProfileList { source: ControlPlaneError },

    #[snafu(display("Query error: {source}"))]
    Query { source: ControlPlaneError },

    #[snafu(display("Failed to update table properties"))]
    TablePropertiesUpdate { source: CatalogError },

    #[snafu(display("Failed to upload data to table: {source}"))]
    DataUpload { source: ControlPlaneError },

    #[snafu(display("Failed to create warehouse"))]
    WarehouseCreate { source: ControlPlaneError },

    #[snafu(display("Warehouse already exists with name {name}"))]
    WarehouseAlreadyExists { name: String },

    #[snafu(display("Failed to delete warehouse with id {id}"))]
    WarehouseDelete { id: Uuid, source: ControlPlaneError },

    #[snafu(display("Malformed namespace ident"))]
    MalformedNamespaceIdent { source: iceberg::Error },

    #[snafu(display("Invalid Iceberg snapshot timestamp"))]
    InvalidIcebergSnapshotTimestamp { source: iceberg::Error },

    #[snafu(display("Malformed multipart message"))]
    MalformedMultipart {
        source: axum::extract::multipart::MultipartError,
    },

    #[snafu(display("Malformed file upload request"))]
    MalformedFileUploadRequest,

    #[snafu(display("Failed to parse table metadata"))]
    ParseTableMetadata {
        source: Box<dyn std::error::Error + Send + Sync>,
        field: String,
    },

    #[snafu(display("Missing Session ID"))]
    MissingSessionId,
}

pub type NexusResult<T> = std::result::Result<T, NexusError>;

impl ToSchema for NexusError {}

impl PartialSchema for NexusError {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::ObjectBuilder::new()
            .property(
                "error",
                utoipa::openapi::ObjectBuilder::new().schema_type(utoipa::openapi::Type::String),
            )
            .required("error")
            .into()
    }
}

impl IntoResponse for NexusError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::StorageProfileFetch { id: _, source }
            | Self::WarehouseFetch { id: _, source }
            | Self::StorageProfileDelete { id: _, source } => match source {
                ControlPlaneError::StorageProfileNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::DatabaseFetch { .. }
            | Self::WarehouseList { .. }
            | Self::DatabaseModelList { .. }
            | Self::NamespaceList { .. }
            | Self::TableList { .. }
            | Self::DatabaseCreate { .. }
            | Self::StorageProfileCreate { .. }
            | Self::Query { .. }
            | Self::StorageProfileList { .. }
            | Self::DataUpload { .. }
            | Self::WarehouseCreate { .. }
            | Self::InvalidIcebergSnapshotTimestamp { .. }
            | Self::ParseTableMetadata { .. } => StatusCode::INTERNAL_SERVER_ERROR,

            Self::DatabaseAlreadyExists { .. } | Self::WarehouseAlreadyExists { .. } => {
                StatusCode::CONFLICT
            }

            Self::MalformedNamespaceIdent { .. }
            | Self::MalformedMultipart { .. }
            | Self::MalformedFileUploadRequest
            | Self::MissingSessionId => StatusCode::BAD_REQUEST,

            Self::TableFetch { id: _, source }
            | Self::TableDelete { source }
            | Self::TablePropertiesUpdate { source } => match source {
                CatalogError::TableNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },

            Self::TableCreate { source } | Self::TableRegister { source } => match source {
                CatalogError::TableAlreadyExists { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },

            Self::DatabaseDelete { ident: _, source } => match source {
                CatalogError::DatabaseNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },

            Self::WarehouseDelete { id: _, source } => match source {
                ControlPlaneError::WarehouseNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        };

        (status, self.to_string()).into_response()
    }
}
