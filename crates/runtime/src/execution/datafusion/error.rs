// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::common::error::DataFusionError;
use iceberg_rust::spec::schema::SchemaBuilderError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum SQLError {
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

pub type SQLResult<T> = std::result::Result<T, SQLError>;
