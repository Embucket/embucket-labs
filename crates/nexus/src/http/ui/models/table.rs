use crate::http::ui::models::database::CompactionSummary;
use crate::http::ui::models::metadata::TableMetadataWrapper;
use crate::http::ui::models::storage_profile::StorageProfile;
use catalog::models as CatalogModels;
use iceberg::spec::TableMetadata;
use iceberg::spec::{Schema, SortOrder, UnboundPartitionSpec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::openapi::{ArrayBuilder, ObjectBuilder, RefOr, Schema as OpenApiSchema};
use utoipa::{PartialSchema, ToSchema};
use uuid::Uuid;
use validator::Validate;

pub fn get_table_id(ident: CatalogModels::TableIdent) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, ident.table.to_string().as_bytes())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
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
    fn from(schema: TableCreatePayload) -> Self {
        catalog::models::TableCreation {
            name: schema.name,
            location: schema.location,
            schema: schema.schema.0,
            partition_spec: schema.partition_spec.map(|x| x.0),
            sort_order: schema.sort_order.map(|x| x.0),
            properties: schema.properties.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
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
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<catalog::models::Table> for Table {
    fn from(table: catalog::models::Table) -> Self {
        Self {
            id: get_table_id(table.clone().ident),
            name: table.ident.table,
            storage_profile: Default::default(),
            database_name: Default::default(),
            warehouse_id: Default::default(),
            properties: Default::default(),
            metadata: TableMetadataWrapper(table.metadata.clone()),
            metadata_location: table.metadata_location,
            created_at: Default::default(),
            updated_at: Default::default(),
            statistics: Statistics::from_table_metadata(&table.metadata),
            compaction_summary: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Statistics {
    pub commit_count: i32,
    pub op_append_count: i32,
    pub op_overwrite_count: i32,
    pub op_delete_count: i32,
    pub op_replace_count: i32,
    pub total_bytes: i32,
    pub bytes_added: i32,
    pub bytes_removed: i32,
    pub total_rows: i32,
    pub rows_added: i32,
    pub rows_deleted: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_count: Option<i32>,
}

impl Statistics {
    pub fn from_table_metadata(metadata: &TableMetadata) -> Statistics {
        let mut total_bytes = 0;
        let mut total_rows = 0;
        let mut rows_deleted = 0;
        let mut commit_count = 0;
        let mut op_append_count = 0;
        let mut op_overwrite_count = 0;
        let mut op_delete_count = 0;
        let mut op_replace_count = 0;
        let mut bytes_added = 0;
        let mut rows_added = 0;

        if let Some(latest_snapshot) = metadata.current_snapshot() {
            total_bytes = latest_snapshot
                .summary()
                .other
                .get("total-files-size")
                .unwrap()
                .parse::<i32>()
                .unwrap();
            total_rows = latest_snapshot
                .summary()
                .other
                .get("total-records")
                .unwrap()
                .parse::<i32>()
                .unwrap();
            rows_deleted = latest_snapshot
                .summary()
                .other
                .get("removed-records")
                .unwrap()
                .parse::<i32>()
                .unwrap();
        };

        metadata.snapshots().for_each(|snapshot| {
            let summary = snapshot.summary();
            commit_count += 1;
            bytes_added += summary
                .other
                .get("added-files-size")
                .unwrap()
                .parse::<i32>()
                .unwrap();
            rows_added += summary
                .other
                .get("added-records")
                .unwrap()
                .parse::<i32>()
                .unwrap();
            match summary.operation {
                iceberg::spec::Operation::Append => op_append_count += 1,
                iceberg::spec::Operation::Overwrite => op_overwrite_count += 1,
                iceberg::spec::Operation::Delete => op_delete_count += 1,
                iceberg::spec::Operation::Replace => op_replace_count += 1,
            }
        });

        Statistics {
            commit_count,
            op_append_count,
            op_overwrite_count,
            op_delete_count,
            op_replace_count,
            total_bytes,
            bytes_added,
            bytes_removed: 0,
            total_rows,
            rows_added,
            rows_deleted,
            table_count: Option::from(1),
            database_count: None,
        }
    }

    pub fn aggregate(&self, other: &Statistics) -> Statistics {
        Statistics {
            commit_count: self.commit_count + other.commit_count,
            op_append_count: self.op_append_count + other.op_append_count,
            op_overwrite_count: self.op_overwrite_count + other.op_overwrite_count,
            op_delete_count: self.op_delete_count + other.op_delete_count,
            op_replace_count: self.op_replace_count + other.op_replace_count,
            total_bytes: self.total_bytes + other.total_bytes,
            bytes_added: self.bytes_added + other.bytes_added,
            bytes_removed: self.bytes_removed + other.bytes_removed,
            total_rows: self.total_rows + other.total_rows,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableQueryRequest {
    pub query: String,
}

impl TableQueryRequest {
    #[allow(clippy::new_without_default)]
    pub fn new(query: String) -> TableQueryRequest {
        TableQueryRequest { query }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableQueryResponse {
    pub id: Uuid,
    pub query: String,
    pub result: String,
}

impl TableQueryResponse {
    #[allow(clippy::new_without_default)]
    pub fn new(id: Uuid, query: String, result: String) -> TableQueryResponse {
        TableQueryResponse { id, query, result }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaWrapper(Schema);

impl ToSchema for SchemaWrapper {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("SchemaWrapper")
    }
}
impl PartialSchema for SchemaWrapper {
    fn schema() -> RefOr<utoipa::openapi::Schema> {
        RefOr::from(utoipa::openapi::Schema::Object(
            ObjectBuilder::new()
                .property("r#struct", OpenApiSchema::Object(Default::default()))
                .property("schema_id", i32::schema())
                .property("highest_field_id", i32::schema())
                .property(
                    "identifier_field_ids",
                    OpenApiSchema::Array(ArrayBuilder::new().items(i32::schema()).build()),
                )
                .property("alias_to_id", OpenApiSchema::Object(Default::default()))
                .property("id_to_field", OpenApiSchema::Object(Default::default()))
                .property("name_to_id", OpenApiSchema::Object(Default::default()))
                .property(
                    "lowercase_name_to_id",
                    OpenApiSchema::Object(Default::default()),
                )
                .property("id_to_name", OpenApiSchema::Object(Default::default()))
                .property(
                    "field_id_to_accessor",
                    OpenApiSchema::Object(Default::default()),
                )
                .build(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnboundPartitionSpecWrapper(UnboundPartitionSpec);

impl ToSchema for UnboundPartitionSpecWrapper {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("UnboundPartitionSpecWrapper")
    }
}
impl PartialSchema for UnboundPartitionSpecWrapper {
    fn schema() -> RefOr<utoipa::openapi::Schema> {
        RefOr::from(utoipa::openapi::Schema::Object(
            ObjectBuilder::new()
                .property("spec_id", i32::schema())
                .property(
                    "fields",
                    OpenApiSchema::Array(ArrayBuilder::new().items(String::schema()).build()),
                )
                .build(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SortOrderWrapper(SortOrder);

impl ToSchema for SortOrderWrapper {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("SortOrderWrapper")
    }
}
impl PartialSchema for SortOrderWrapper {
    fn schema() -> RefOr<utoipa::openapi::Schema> {
        RefOr::from(utoipa::openapi::Schema::Object(
            ObjectBuilder::new()
                .property("order_id", i64::schema())
                .property(
                    "fields",
                    OpenApiSchema::Array(ArrayBuilder::new().items(String::schema()).build()),
                )
                .build(),
        ))
    }
}
