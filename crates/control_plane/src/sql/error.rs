use datafusion::error::DataFusionError;
use iceberg_rust::spec::schema::SchemaBuilderError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError},

    #[snafu(display("No Table Provider found for table: {table_name}"))]
    TableProviderNotFound { table_name: String },

    #[snafu(display("Cannot register UDF JSON functions"))]
    RegisterUDFJSON { source: DataFusionError },

    #[snafu(display("Schema builder error: {source}"))]
    SchemaBuilder { source: SchemaBuilderError },

    #[snafu(display("Warehouse not found for name {name}"))]
    WarehouseNotFound { name: String },

    #[snafu(display("Warehouse {warehouse_name} is not an Iceberg catalog"))]
    IcebergCatalogNotFound { warehouse_name: String },

    #[snafu(display("Iceberg error: {source}"))]
    Iceberg { source: iceberg_rust::error::Error },
}

pub type Result<T> = std::result::Result<T, Error>;