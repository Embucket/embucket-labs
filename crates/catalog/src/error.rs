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
