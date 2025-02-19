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

use datafusion::error::DataFusionError;
#[warn(dead_code)]
use quick_xml::de::from_str;
use rusoto_core::RusotoError;
use serde::Deserialize;
use snafu::prelude::*;
use tokio::sync::TryLockError;
use uuid::Uuid;

pub type ControlPlaneResult<T> = Result<T, ControlPlaneError>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum ControlPlaneError {
    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError },

    #[snafu(display("DataFusion query error: {source}, query: {query}"))]
    DataFusionQuery {
        source: DataFusionError,
        query: String,
    },

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

    #[snafu(display("Storage profile not found for id {id}"))]
    StorageProfileNotFound { id: Uuid },

    #[snafu(display("Storage profile {} already in use", id))]
    StorageProfileInUse { id: Uuid },

    #[snafu(display("Invalid storage profile: {source}"))]
    InvalidStorageProfile {
        source: crate::models::ControlPlaneModelError,
    },

    #[snafu(display("Unable to create Warehouse from request: {source}"))]
    InvalidCreateWarehouse {
        source: crate::models::ControlPlaneModelError,
    },

    #[snafu(display("Warehouse not found: {id}"))]
    WarehouseNotFound { id: Uuid },

    #[snafu(display("Warehouse name not found: '{name}'"))]
    WarehouseNameNotFound { name: String },

    #[snafu(display("Unable to delete Warehouse, not empty: {id}"))]
    WarehouseNotEmpty { id: Uuid },

    #[snafu(display("Missing storage endpoint URL"))]
    MissingStorageEndpointURL,

    #[snafu(display("Invalid storage endpoint URL: {url}, source: {source}"))]
    InvalidStorageEndpointURL {
        url: String,
        source: url::ParseError,
    },

    // This should be refined later
    #[snafu(display("Unspported Authentication method: {method}"))]
    UnsupportedAuthenticationMethod { method: String },

    #[snafu(display("Invalid or missing credentials"))]
    CredentialsValidationFailed,

    #[snafu(display("Invalid TLS configuration: {source}"))]
    InvalidTLSConfiguration {
        source: rusoto_core::request::TlsError,
    },

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

    #[snafu(display("Execution error: {source}"))]
    Execution {
        source: runtime::datafusion::error::IceBucketSQLError,
    },

    #[snafu(display("Unable to lock DataFusion context list"))]
    ContextListLock { source: TryLockError },

    #[snafu(display("Missing DataFusion session for id {id}"))]
    MissingDataFusionSession { id: String },
}

impl From<utils::Error> for ControlPlaneError {
    fn from(e: utils::Error) -> Self {
        Self::SlateDB { source: e }
    }
}

impl<T: std::error::Error + Send + Sync + 'static> From<RusotoError<T>> for ControlPlaneError {
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
                if let Ok(s3_error) = from_str::<S3ErrorMessage>(body_string.as_ref()) {
                    Self::S3Unknown {
                        code: s3_error.code,
                        message: s3_error.message,
                    }
                } else {
                    Self::S3Unknown {
                        code: "unknown".to_string(),
                        message: body_string.to_string(),
                    }
                }
            }
            _ => Self::S3 {
                source: Box::new(e),
            },
        }
    }
}

impl From<DataFusionError> for ControlPlaneError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusion { source: err }
    }
}

impl From<icelake::Error> for ControlPlaneError {
    fn from(err: icelake::Error) -> Self {
        Self::IceLake { source: err }
    }
}
