use datafusion::common::error::DataFusionError;
use iceberg_rust::spec::schema::SchemaBuilderError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum IceBucketSQLError {
    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError },

    #[snafu(display("No Table Provider found for table: {table_name}"))]
    TableProviderNotFound { table_name: String },

    #[snafu(display("Cannot register UDF functions"))]
    RegisterUDF { source: DataFusionError },

    #[snafu(display("Schema builder error: {source}"))]
    SchemaBuilder { source: SchemaBuilderError },

    #[snafu(display("Warehouse not found for name {name}"))]
    WarehouseNotFound { name: String },

    #[snafu(display("Warehouse {warehouse_name} is not an Iceberg catalog"))]
    IcebergCatalogNotFound { warehouse_name: String },

    #[snafu(display("Iceberg error: {source}"))]
    Iceberg { source: iceberg_rust::error::Error },

    #[snafu(display("Iceberg spec error: {source}"))]
    IcebergSpec {
        source: iceberg_rust::spec::error::Error,
    },

    #[snafu(display("Invalid precision: {precision}"))]
    InvalidPrecision { precision: String },

    #[snafu(display("Invalid scale: {scale}"))]
    InvalidScale { scale: String },

    #[snafu(display("Invalid table identifier: {ident}"))]
    InvalidIdentifier { ident: String },

    #[snafu(display("Not implemented: {message}"))]
    NotImplemented { message: String },
}

pub type IceBucketSQLResult<T> = std::result::Result<T, IceBucketSQLError>;
