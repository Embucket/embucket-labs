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

use crate::execution::query::IceBucketQueryContext;
use crate::execution::service::ExecutionService;
use crate::execution::utils::{Config, DataSerializationFormat};
use crate::SlateDBMetastore;

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_execute_always_returns_schema() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let execution_svc = ExecutionService::new(
        metastore.clone(),
        Config {
            dbt_serialization_format: DataSerializationFormat::Json,
        },
    );

    execution_svc
        .create_session("test_session_id".to_string())
        .await
        .expect("Failed to create session");

    let (_, columns) = execution_svc
        .query(
            "test_session_id",
            "SELECT 1 AS a, 2.0 AS b, '3' AS c WHERE False",
            IceBucketQueryContext::default(),
        )
        .await
        .expect("Failed to execute query");
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].r#type, "fixed");
    assert_eq!(columns[1].r#type, "real");
    assert_eq!(columns[2].r#type, "text");
}
