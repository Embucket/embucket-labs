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

use std::backtrace::Backtrace;

use crate::http::error::ErrorResponse;
use axum::{response::IntoResponse, Json};
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
    InvalidTableIdentifier { ident: String },

    #[snafu(display("Invalid schema identifier: {ident}"))]
    InvalidSchemaIdentifier { ident: String },

    #[snafu(display("Invalid file path: {path}"))]
    InvalidFilePath { path: String },

    #[snafu(display("Invalid bucket identifier: {ident}"))]
    InvalidBucketIdentifier { ident: String },

    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: arrow::error::ArrowError },

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
    Metastore {
        source: icebucket_metastore::error::MetastoreError,
    },

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

    #[snafu(display("Object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists { type_name: String, name: String },

    #[snafu(display("Unsupported file format {format}"))]
    UnsupportedFileFormat { format: String },

    #[snafu(display("Cannot refresh catalog list"))]
    RefreshCatalogList { message: String },

    #[snafu(display("URL Parsing error: {source}"))]
    UrlParse { source: url::ParseError },

    #[snafu(display("Threaded Job error: {source}: {backtrace}"))]
    JobError {
        source: crate::execution::dedicated_executor::JobError,
        backtrace: Backtrace,
    },
}

pub type ExecutionResult<T> = std::result::Result<T, ExecutionError>;

impl IntoResponse for ExecutionError {
    fn into_response(self) -> axum::response::Response {
        let er = ErrorResponse {
            message: self.to_string(),
            status_code: http::StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        };
        (http::StatusCode::INTERNAL_SERVER_ERROR, Json(er)).into_response()
    }
}
