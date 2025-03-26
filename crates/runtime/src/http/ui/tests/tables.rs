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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::http::error::ErrorResponse;
use crate::http::ui::databases::models::{DatabasePayload, DatabaseResponse, DatabasesResponse};
use crate::http::ui::tests::common::{ui_test_op, Entity, Op};
use crate::http::ui::volumes::models::{VolumePayload, VolumeResponse};
use crate::http::ui::tables::models::{TableColumn, GetTableResponse};
use crate::tests::run_icebucket_test_server;
use icebucket_metastore::IceBucketVolumeType;
use icebucket_metastore::{IceBucketDatabase, IceBucketVolume};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_tables() {

}