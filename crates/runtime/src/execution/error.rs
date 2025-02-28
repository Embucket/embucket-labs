use datafusion_common::DataFusionError;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ExecutionError {
    #[snafu(display("Cannot register UDF functions"))]
    RegisterUDF { source: DataFusionError },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError },

    #[snafu(display("Invalid table identifier: {ident}"))]
    InvalidIdentifier { ident: String },

    //TODO: Rename this to match snowflake nomenclature
    #[snafu(display("Warehouse not found for name {name}"))]
    WarehouseNotFound { name: String },

    //TODO: Remove this error or rename it
    #[snafu(display("Warehouse {warehouse_name} is not an Iceberg catalog"))]
    IcebergCatalogNotFound { warehouse_name: String },

    //TODO: Remove this error
    #[snafu(display("Iceberg error: {source}"))]
    Iceberg { source: iceberg_rust::error::Error },

    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: arrow::error::ArrowError },

    //TODO: Remove this error
    #[snafu(display("Iceberg spec error: {source}"))]
    IcebergSpec {
        source: iceberg_rust::spec::error::Error,
    },

    #[snafu(display("No Table Provider found for table: {table_name}"))]
    TableProviderNotFound { table_name: String },

    #[snafu(display("Missing DataFusion session for id {id}"))]
    MissingDataFusionSession { id: String },

    #[snafu(display("DataFusion query error: {source}, query: {query}"))]
    DataFusionQuery {
        source: DataFusionError,
        query: String,
    },

    #[snafu(display("Error encoding UTF8 string: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },

    #[snafu(display("Metastore error: {source}"))]
    Metastore { source: icebucket_metastore::error::MetastoreError },

    #[snafu(display("Database {db} not found"))]
    DatabaseNotFound { db: String },

    #[snafu(display("Table {table} not found"))]
    TableNotFound { table: String },

    #[snafu(display("Schema {schema} not found"))]
    SchemaNotFound { schema: String },

    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound { volume: String },

    #[snafu(display("Object store error: {source}"))]
    ObjectStore { source: object_store::Error },
}

pub type ExecutionResult<T> = std::result::Result<T, ExecutionError>;