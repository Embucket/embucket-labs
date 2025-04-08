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
use datafusion::assert_batches_eq;
use icebucket_metastore::models::table::IceBucketTableIdent;
use icebucket_metastore::Metastore;
use icebucket_metastore::{
    IceBucketDatabase, IceBucketSchema, IceBucketSchemaIdent, IceBucketVolume,
};

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

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_service_upload_file() {
    let metastore = SlateDBMetastore::new_in_memory().await;

    metastore
        .create_volume(
            &"test_volume".to_string(),
            IceBucketVolume::new(
                "test_volume".to_string(),
                icebucket_metastore::IceBucketVolumeType::Memory,
            ),
        )
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(
            &"icebucket".to_string(),
            IceBucketDatabase {
                ident: "icebucket".to_string(),
                properties: None,
                volume: "test_volume".to_string(),
            },
        )
        .await
        .expect("Failed to create database");
    let schema_ident = IceBucketSchemaIdent {
        database: "icebucket".to_string(),
        schema: "public".to_string(),
    };
    metastore
        .create_schema(
            &schema_ident.clone(),
            IceBucketSchema {
                ident: schema_ident,
                properties: None,
            },
        )
        .await
        .expect("Failed to create schema");

    let file_name = "test.csv";
    let table_ident = IceBucketTableIdent {
        database: "icebucket".to_string(),
        schema: "public".to_string(),
        table: "target_table".to_string(),
    };

    // Create CSV data in memory
    let csv_content = "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300";
    let data = csv_content.as_bytes().to_vec();

    let execution_svc = ExecutionService::new(
        metastore.clone(),
        Config {
            dbt_serialization_format: DataSerializationFormat::Json,
        },
    );

    let session_id = "test_session_id";
    execution_svc
        .create_session(session_id.to_string())
        .await
        .expect("Failed to create session");

    execution_svc
        .upload_data_to_table(session_id, &table_ident, data.clone().into(), file_name)
        .await
        .expect("Failed to upload file");

    // Verify that the file was uploaded successfully by running select * from the table
    let query = format!("SELECT * FROM {}", table_ident.table);
    let (rows, _) = execution_svc
        .query(session_id, &query, IceBucketQueryContext::default())
        .await
        .expect("Failed to execute query");

    assert_batches_eq!(
        &[
            "+----+-------+-------+",
            "| id | name  | value |",
            "+----+-------+-------+",
            "| 1  | test1 | 100   |",
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "+----+-------+-------+",
        ],
        &rows
    );

    execution_svc
        .upload_data_to_table(session_id, &table_ident, data.into(), file_name)
        .await
        .expect("Failed to upload file");

    // Verify that the file was uploaded successfully by running select * from the table
    let query = format!("SELECT * FROM {}", table_ident.table);
    let (rows, _) = execution_svc
        .query(session_id, &query, IceBucketQueryContext::default())
        .await
        .expect("Failed to execute query");

    assert_batches_eq!(
        &[
            "+----+-------+-------+",
            "| id | name  | value |",
            "+----+-------+-------+",
            "| 1  | test1 | 100   |",
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "| 1  | test1 | 100   |",
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "+----+-------+-------+",
        ],
        &rows
    );
}
