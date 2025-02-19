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

// Errors that are specific to the `models` crate
#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum ControlPlaneModelError {
    #[snafu(display("Invalid bucket name `{bucket_name}`. Reason: {reason}"))]
    InvalidBucketName { bucket_name: String, reason: String },

    #[snafu(display("Invalid region name `{region}`. Reason: {reason}"))]
    InvalidRegionName { region: String, reason: String },

    #[snafu(display("Invalid directory `{directory}`"))]
    InvalidDirectory { directory: String },

    #[snafu(display("Invalid endpoint url `{url}`"))]
    InvalidEndpointUrl {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display("Cloud providerNot implemented"))]
    CloudProviderNotImplemented { provider: String },

    #[snafu(display("Unable to parse key `{key}`"))]
    UnableToParseConfiguration {
        key: String,
        source: Box<dyn std::error::Error + Send>,
    },

    #[snafu(display("Role-based credentials aren't supported"))]
    RoleBasedCredentialsNotSupported,

    #[snafu(display("Missing credentials for {profile_type} profile type"))]
    MissingCredentials { profile_type: String },

    #[snafu(display("Missing environment variable `{var}`"))]
    MissingEnvironmentVariable {
        source: std::env::VarError,
        var: String,
    },

    // Duplicated in `control_plane` crate, needs refactoring
    #[snafu(display("Object store error: {source}"))]
    ObjectStore { source: object_store::Error },
}

pub type ControlPlaneModelResult<T> = Result<T, ControlPlaneModelError>;
