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
use crate::http::ui::models::database::CompactionSummary;
use crate::http::ui::models::storage_profile::StorageProfile;
use catalog::models as CatalogModels;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::openapi::SchemaFormat::KnownFormat;
use utoipa::openapi::{ObjectBuilder, Ref, RefOr, Type};
use utoipa::{PartialSchema, ToSchema};
use uuid::Uuid;
use validator::Validate;

#[must_use]
pub fn get_table_id(ident: &CatalogModels::TableIdent) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, ident.table.as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMetadataWrapper(pub TableMetadata);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnboundPartitionSpecWrapper(pub(crate) UnboundPartitionSpec);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SortOrderWrapper(pub(crate) SortOrder);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableRegisterRequest {
    pub name: String,
    pub metadata_location: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct TableCreatePayload {
    pub name: String,
    pub location: Option<String>,
    pub schema: SchemaWrapper,
    pub partition_spec: Option<UnboundPartitionSpecWrapper>,
    pub sort_order: Option<SortOrderWrapper>,
    pub stage_create: Option<bool>,
    pub properties: Option<HashMap<String, String>>,
}

impl From<TableCreatePayload> for catalog::models::TableCreation {
    fn from(payload: TableCreatePayload) -> Self {
        Self {
            name: payload.name,
            location: payload.location,
            schema: payload.schema.0,
            partition_spec: payload.partition_spec.map(|x| x.0),
            sort_order: payload.sort_order.map(|x| x.0),
            properties: payload.properties.unwrap_or_default(),
        }
    }
}

impl ToSchema for TableCreatePayload {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("TableCreatePayload")
    }
}

impl PartialSchema for TableCreatePayload {
    fn schema() -> RefOr<utoipa::openapi::Schema> {
        RefOr::from(utoipa::openapi::Schema::Object(
            ObjectBuilder::new()
                .property("name", String::schema())
                .property("location", Option::<String>::schema())
                .property("schema", Ref::new("#/components/schemas/Schema"))
                .property(
                    "partition_spec",
                    Ref::new("#/components/schemas/PartitionSpec"),
                )
                .property("sort_order", Ref::new("#/components/schemas/SortOrder"))
                .property("stage_create", Option::<bool>::schema())
                .property("properties", Option::<HashMap<String, String>>::schema())
                .required("name")
                .required("schema")
                .build(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    pub id: Uuid,
    pub name: String,
    pub storage_profile: StorageProfile,
    pub database_name: String,
    pub warehouse_id: Uuid,
    pub properties: HashMap<String, String>,
    pub metadata: TableMetadataWrapper,
    pub metadata_location: String,
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Table {
    pub fn with_details(
        &mut self,
        warehouse_id: Uuid,
        profile: StorageProfile,
        database_name: String,
    ) {
        self.storage_profile = profile;
        self.warehouse_id = warehouse_id;
        self.database_name = database_name;
        self.properties = self.properties.clone();
        if let Some(created_at) = self.properties.get("created_at") {
            if let Ok(created_at) = DateTime::parse_from_rfc3339(created_at) {
                self.created_at = DateTime::from(created_at);
            }
        }
        if let Some(updated_at) = self.properties.get("updated_at") {
            if let Ok(updated_at) = DateTime::parse_from_rfc3339(updated_at) {
                self.updated_at = DateTime::from(updated_at);
            }
        }
    }
}

impl From<catalog::models::Table> for Table {
    fn from(table: catalog::models::Table) -> Self {
        Self {
            id: get_table_id(&table.ident),
            name: table.ident.table,
            storage_profile: StorageProfile::default(),
            database_name: String::default(),
            warehouse_id: Uuid::default(),
            properties: table.properties,
            metadata: TableMetadataWrapper(table.metadata.clone()),
            metadata_location: table.metadata_location,
            created_at: DateTime::default(),
            updated_at: DateTime::default(),
            statistics: Statistics::from_table_metadata(&table.metadata),
            compaction_summary: None,
        }
    }
}

impl ToSchema for Table {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Table")
    }
}
impl PartialSchema for Table {
    fn schema() -> RefOr<utoipa::openapi::Schema> {
        RefOr::from(utoipa::openapi::Schema::Object(
            ObjectBuilder::new()
                .property(
                    "id",
                    ObjectBuilder::new()
                        .schema_type(Type::String)
                        .format(Some(KnownFormat(utoipa::openapi::KnownFormat::Uuid))),
                )
                .property("name", String::schema())
                .property(
                    "storageProfile",
                    Ref::new("#/components/schemas/StorageProfile"),
                )
                .property("databaseName", String::schema())
                .property(
                    "warehouseId",
                    ObjectBuilder::new()
                        .schema_type(Type::String)
                        .format(Some(KnownFormat(utoipa::openapi::KnownFormat::Uuid))),
                )
                .property("properties", HashMap::<String, String>::schema())
                .property("metadata", Ref::new("#/components/schemas/TableMetadata"))
                .property("metadataLocation", String::schema())
                .property("statistics", Ref::new("#/components/schemas/Statistics"))
                .property(
                    "compactionSummary",
                    Ref::new("#/components/schemas/CompactionSummary"),
                )
                .property(
                    "createdAt",
                    ObjectBuilder::new()
                        .schema_type(Type::String)
                        .format(Some(KnownFormat(utoipa::openapi::KnownFormat::DateTime))),
                )
                .property(
                    "updatedAt",
                    ObjectBuilder::new()
                        .schema_type(Type::String)
                        .format(Some(KnownFormat(utoipa::openapi::KnownFormat::DateTime))),
                )
                .required("name")
                .build(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Statistics {
    pub commit_count: i32,
    pub total_bytes: i64,
    pub total_rows: i64,
    pub total_files: i32,
    pub total_snapshots_files: i32,
    pub op_append_count: i64,
    pub op_overwrite_count: i64,
    pub op_delete_count: i64,
    pub op_replace_count: i64,
    pub bytes_added: i64,
    pub bytes_removed: i64,
    pub rows_added: i64,
    pub rows_deleted: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_count: Option<i32>,
}

impl Statistics {
    #[must_use]
    pub fn from_table_metadata(metadata: &TableMetadata) -> Self {
        let mut commit_count = 0;
        let mut total_bytes = 0;
        let mut total_rows = 0;
        let mut total_files = 0;
        let mut total_snapshots_files = 0;
        let mut rows_deleted = 0;
        let mut bytes_removed = 0;
        let mut rows_added = 0;
        let mut bytes_added = 0;
        let mut op_append_count = 0;
        let mut op_overwrite_count = 0;
        let mut op_delete_count = 0;
        let mut op_replace_count = 0;
        

        if let Some(latest_snapshot) = metadata.current_snapshot() {
            total_bytes = latest_snapshot
                .summary()
                .other
                .get("total-files-size")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            total_rows = latest_snapshot
                .summary()
                .other
                .get("total-records")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            total_files = latest_snapshot
                .summary()
                .other
                .get("total-data-files")
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or(0);
        };

        metadata.snapshots().for_each(|snapshot| {
            let summary = snapshot.summary();
            commit_count += 1;
            bytes_added += summary
                .other
                .get("added-files-size")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            rows_added += summary
                .other
                .get("added-records")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            bytes_removed += summary
                .other
                .get("removed-files-size")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            rows_deleted += summary
                .other
                .get("deleted-records")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            total_snapshots_files += summary
                .other
                .get("total-data-files")
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or(0);
            
            match summary.operation {
                iceberg::spec::Operation::Append => op_append_count += 1,
                iceberg::spec::Operation::Overwrite => op_overwrite_count += 1,
                iceberg::spec::Operation::Delete => op_delete_count += 1,
                iceberg::spec::Operation::Replace => op_replace_count += 1,
            }
        });

        Self {
            commit_count,
            op_append_count,
            op_overwrite_count,
            op_delete_count,
            op_replace_count,
            total_bytes,
            total_files,
            total_snapshots_files,
            bytes_added,
            bytes_removed,
            total_rows,
            rows_added,
            rows_deleted,
            table_count: Option::from(1),
            database_count: None,
        }
    }

    #[must_use]
    pub fn aggregate(&self, other: &Self) -> Self {
        Self {
            commit_count: self.commit_count + other.commit_count,
            op_append_count: self.op_append_count + other.op_append_count,
            op_overwrite_count: self.op_overwrite_count + other.op_overwrite_count,
            op_delete_count: self.op_delete_count + other.op_delete_count,
            op_replace_count: self.op_replace_count + other.op_replace_count,
            total_bytes: self.total_bytes + other.total_bytes,
            bytes_added: self.bytes_added + other.bytes_added,
            bytes_removed: self.bytes_removed + other.bytes_removed,
            total_rows: self.total_rows + other.total_rows,
            total_files: self.total_files + other.total_files,
            total_snapshots_files: self.total_snapshots_files + other.total_snapshots_files,
            rows_added: self.rows_added + other.rows_added,
            rows_deleted: self.rows_deleted + other.rows_deleted,
            table_count: self.table_count.map_or(other.table_count, |count| {
                Some(count + other.table_count.unwrap_or(0))
            }),
            database_count: self.database_count.map_or(other.database_count, |count| {
                Some(count + other.database_count.unwrap_or(0))
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryPayload {
    pub query: String,
    pub context: Option<HashMap<String, String>>,
}

impl QueryPayload {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(query: String) -> Self {
        Self { query, context: None }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub query: String,
    pub result: String,
    pub duration_seconds: f32,
}

impl QueryResponse {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(query: String, result: String, duration_seconds: f32) -> Self {
        Self {
            query,
            result,
            duration_seconds,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaWrapper(Schema);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableUploadPayload {
    #[schema(format = "binary")]
    pub upload_file: String,
}
