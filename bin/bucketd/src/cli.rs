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

use clap::{Parser, ValueEnum};
use object_store::{
    aws::AmazonS3Builder, aws::S3ConditionalPut, local::LocalFileSystem, memory::InMemory,
    ObjectStore, Result as ObjectStoreResult,
};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about=None)]
pub struct IceBucketOpts {
    #[arg(
        short,
        long,
        value_enum,
        env = "OBJECT_STORE_BACKEND",
        help = "Backend to use for state storage"
    )]
    backend: StoreBackend,

    #[arg(
        long,
        env = "AWS_ACCESS_KEY_ID",
        required_if_eq("backend", "s3"),
        help = "AWS Access Key ID",
        help_heading = "S3 Backend Options"
    )]
    access_key_id: Option<String>,
    #[arg(
        long,
        env = "AWS_SECRET_ACCESS_KEY",
        required_if_eq("backend", "s3"),
        help = "AWS Secret Access Key",
        help_heading = "S3 Backend Options"
    )]
    secret_access_key: Option<String>,
    #[arg(
        long,
        env = "AWS_REGION",
        required_if_eq("backend", "s3"),
        help = "AWS Region",
        help_heading = "S3 Backend Options"
    )]
    region: Option<String>,
    #[arg(
        long,
        env = "S3_BUCKET",
        required_if_eq("backend", "s3"),
        help = "S3 Bucket Name",
        help_heading = "S3 Backend Options"
    )]
    bucket: Option<String>,
    #[arg(
        long,
        env = "S3_ENDPOINT",
        help = "S3 Endpoint (Optional)",
        help_heading = "S3 Backend Options"
    )]
    endpoint: Option<String>,
    #[arg(
        long,
        env = "S3_ALLOW_HTTP",
        help = "Allow HTTP for S3 (Optional)",
        help_heading = "S3 Backend Options"
    )]
    allow_http: Option<bool>,

    #[arg(
        long,
        env = "FILE_STORAGE_PATH",
        required_if_eq("backend", "file"),
        help_heading = "File Backend Options",
        help = "Path to the directory where files will be stored"
    )]
    file_storage_path: Option<PathBuf>,

    #[arg(short, long, env = "SLATEDB_PREFIX")]
    pub slatedb_prefix: String,

    #[arg(
        long,
        env = "BUCKET_HOST",
        default_value = "127.0.0.1",
        help = "Host to bind to"
    )]
    pub host: Option<String>,

    #[arg(
        long,
        env = "BUCKET_PORT",
        default_value = "3000",
        help = "Port to bind to"
    )]
    pub port: Option<u16>,

    #[arg(
        long,
        env = "CORS_ENABLED",
        help = "Enable CORS",
        default_value = "false"
    )]
    pub cors_enabled: Option<bool>,

    #[arg(
        long,
        env = "CORS_ALLOW_ORIGIN",
        required_if_eq("cors_enabled", "true"),
        help = "CORS Allow Origin"
    )]
    pub cors_allow_origin: Option<String>,

    #[arg(
        short,
        long,
        default_value = "json",
        env = "DATA_FORMAT",
        help = "Data serialization format in Snowflake v1 API"
    )]
    pub data_format: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum StoreBackend {
    S3,
    File,
    Memory,
}

impl IceBucketOpts {
    #[allow(clippy::unwrap_used, clippy::as_conversions)]
    pub fn object_store_backend(self) -> ObjectStoreResult<Box<dyn ObjectStore>> {
        match self.backend {
            StoreBackend::S3 => {
                let s3_allow_http = self.allow_http.unwrap_or(false);

                let s3_builder = AmazonS3Builder::new()
                    .with_access_key_id(self.access_key_id.unwrap())
                    .with_secret_access_key(self.secret_access_key.unwrap())
                    .with_region(self.region.unwrap())
                    .with_bucket_name(self.bucket.unwrap())
                    .with_conditional_put(S3ConditionalPut::ETagMatch);

                if let Some(endpoint) = self.endpoint {
                    s3_builder
                        .with_endpoint(&endpoint)
                        .with_allow_http(s3_allow_http)
                        .build()
                        .map(|s3| Box::new(s3) as Box<dyn ObjectStore>)
                } else {
                    s3_builder
                        .build()
                        .map(|s3| Box::new(s3) as Box<dyn ObjectStore>)
                }
            }
            StoreBackend::File => {
                let file_storage_path = self.file_storage_path.unwrap();
                let path = file_storage_path.as_path();
                if !path.exists() || !path.is_dir() {
                    fs::create_dir(path).unwrap();
                }
                LocalFileSystem::new_with_prefix(file_storage_path)
                    .map(|fs| Box::new(fs) as Box<dyn ObjectStore>)
            }
            StoreBackend::Memory => Ok(Box::new(InMemory::new()) as Box<dyn ObjectStore>),
        }
    }
}
