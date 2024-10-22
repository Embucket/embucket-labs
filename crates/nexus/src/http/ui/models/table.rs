use crate::http::ui::models::database::{CompactionSummary, DatabaseExtended};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct TableShort {
    pub id: uuid::Uuid,
    pub name: String,
}

impl TableShort {
    #[allow(clippy::new_without_default)]
    pub fn new(id: uuid::Uuid, name: String) -> TableShort {
        TableShort { id, name }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct TableMetadataV2 {
    pub location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_uuid: Option<uuid::Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_ms: Option<i32>,
    pub last_column_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<Schema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_schema_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_specs: Option<Vec<PartitionSpec>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_spec_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_partition_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<Snapshot>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<SnapshotLogEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_log: Option<Vec<MetadataLogEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_orders: Option<Vec<SortOrder>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_sort_order_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs: Option<std::collections::HashMap<String, SnapshotRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format_version: Option<FormatVersion1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sequence_number: Option<i32>,
}

impl TableMetadataV2 {
    #[allow(clippy::new_without_default)]
    pub fn new(location: String, last_column_id: i32) -> TableMetadataV2 {
        TableMetadataV2 {
            location,
            table_uuid: None,
            last_updated_ms: None,
            last_column_id,
            schemas: None,
            current_schema_id: Some(0),
            partition_specs: None,
            default_spec_id: Some(0),
            last_partition_id: None,
            properties: None,
            current_snapshot_id: None,
            snapshots: None,
            snapshot_log: None,
            metadata_log: None,
            sort_orders: None,
            default_sort_order_id: Some(0),
            refs: None,
            format_version: None,
            last_sequence_number: Some(0),
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash,
)]
pub enum FormatVersion1 {
    #[serde(rename = "2")]
    Variant2,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct MetadataLogEntry {
    pub metadata_file: String,
    pub timestamp_ms: i32,
}

impl MetadataLogEntry {
    #[allow(clippy::new_without_default)]
    pub fn new(metadata_file: String, timestamp_ms: i32) -> MetadataLogEntry {
        MetadataLogEntry {
            metadata_file,
            timestamp_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct PartitionSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<PartitionField>>,
}

impl PartitionSpec {
    #[allow(clippy::new_without_default)]
    pub fn new() -> PartitionSpec {
        PartitionSpec {
            spec_id: Some(0),
            fields: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct Schema {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<Type>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<NestedField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
}

impl Schema {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Schema {
        Schema {
            r#type: None,
            fields: None,
            schema_id: Some(0),
            identifier_field_ids: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct Snapshot {
    pub snapshot_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_list: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
}

impl Snapshot {
    #[allow(clippy::new_without_default)]
    pub fn new(snapshot_id: i32) -> Snapshot {
        Snapshot {
            snapshot_id,
            parent_snapshot_id: None,
            sequence_number: None,
            timestamp_ms: None,
            manifest_list: None,
            summary: None,
            schema_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct SnapshotLogEntry {
    pub snapshot_id: i32,
    pub timestamp_ms: i32,
}

impl SnapshotLogEntry {
    #[allow(clippy::new_without_default)]
    pub fn new(snapshot_id: i32, timestamp_ms: i32) -> SnapshotLogEntry {
        SnapshotLogEntry {
            snapshot_id,
            timestamp_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct SnapshotRef {
    pub snapshot_id: i32,
    pub r#type: SnapshotRefType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i32>,
}

impl SnapshotRef {
    #[allow(clippy::new_without_default)]
    pub fn new(snapshot_id: i32, r#type: SnapshotRefType) -> SnapshotRef {
        SnapshotRef {
            snapshot_id,
            r#type,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, ToSchema,
)]
pub enum SnapshotRefType {
    #[serde(rename = "branch")]
    Branch,
    #[serde(rename = "tag")]
    Tag,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, ToSchema,
)]
pub enum FormatVersion {
    #[serde(rename = "1")]
    Variant1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct NestedField {
    pub id: i32,
    pub name: String,
    pub r#type: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_default: Option<String>,
}

impl NestedField {
    #[allow(clippy::new_without_default)]
    pub fn new(id: i32, name: String, r#type: serde_json::Value) -> NestedField {
        NestedField {
            id,
            name,
            r#type,
            required: Some(false),
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, ToSchema,
)]
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    First,
    #[serde(rename = "nulls-last")]
    Last,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, ToSchema,
)]
pub enum Operation {
    #[serde(rename = "append")]
    Append,
    #[serde(rename = "replace")]
    Replace,
    #[serde(rename = "overwrite")]
    Overwrite,
    #[serde(rename = "delete")]
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub transform: String,
    pub name: String,
}

impl PartitionField {
    #[allow(clippy::new_without_default)]
    pub fn new(source_id: i32, field_id: i32, transform: String, name: String) -> PartitionField {
        PartitionField {
            source_id,
            field_id,
            transform,
            name,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, ToSchema,
)]
pub enum SortDirection {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct SortField {
    pub source_id: i32,
    pub transform: String,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

impl SortField {
    #[allow(clippy::new_without_default)]
    pub fn new(
        source_id: i32,
        transform: String,
        direction: SortDirection,
        null_order: NullOrder,
    ) -> SortField {
        SortField {
            source_id,
            transform,
            direction,
            null_order,
        }
    }
}

/// Describes how the data is sorted within the table.  Users can sort their data within partitions by columns to gain performance.  The order of the sort fields within the list defines the order in which the sort is applied to the data.  Args:   fields (List[SortField]): The fields how the table is sorted.  Keyword Args:   order_id (int): An unique id of the sort-order of a table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct SortOrder {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<SortField>>,
}

impl SortOrder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> SortOrder {
        SortOrder {
            order_id: Some(1),
            fields: None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, ToSchema,
)]
pub enum Type {
    #[serde(rename = "struct")]
    Struct,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct TableEntity {
    pub id: uuid::Uuid,
    pub name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl TableEntity {
    #[allow(clippy::new_without_default)]
    pub fn new(
        id: uuid::Uuid,
        name: String,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
        statistics: Statistics,
    ) -> TableEntity {
        TableEntity {
            id,
            name,
            created_at,
            updated_at,
            statistics,
            compaction_summary: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct TableExtended {
    pub id: uuid::Uuid,
    pub name: String,
    pub database_name: String,
    pub warehouse_id: uuid::Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
    pub metadata: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Statistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub database: DatabaseExtended,
}

impl TableExtended {
    #[allow(clippy::new_without_default)]
    pub fn new(
        id: uuid::Uuid,
        name: String,
        database_name: String,
        warehouse_id: uuid::Uuid,
        metadata: serde_json::Value,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
        database: DatabaseExtended,
    ) -> TableExtended {
        TableExtended {
            id,
            name,
            database_name,
            warehouse_id,
            properties: None,
            metadata,
            statistics: None,
            compaction_summary: None,
            created_at,
            updated_at,
            database,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct TableMetadataV1 {
    pub location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_uuid: Option<uuid::Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_ms: Option<i32>,
    pub last_column_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<Schema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_schema_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_specs: Option<Vec<PartitionSpec>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_spec_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_partition_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<Snapshot>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<SnapshotLogEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_log: Option<Vec<MetadataLogEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_orders: Option<Vec<SortOrder>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_sort_order_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs: Option<std::collections::HashMap<String, SnapshotRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format_version: Option<FormatVersion>,
    pub schema: Schema,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<Vec<serde_json::Value>>,
}

impl TableMetadataV1 {
    #[allow(clippy::new_without_default)]
    pub fn new(location: String, last_column_id: i32, schema: Schema) -> TableMetadataV1 {
        TableMetadataV1 {
            location,
            table_uuid: None,
            last_updated_ms: None,
            last_column_id,
            schemas: None,
            current_schema_id: Some(0),
            partition_specs: None,
            default_spec_id: Some(0),
            last_partition_id: None,
            properties: None,
            current_snapshot_id: None,
            snapshots: None,
            snapshot_log: None,
            metadata_log: None,
            sort_orders: None,
            default_sort_order_id: Some(0),
            refs: None,
            format_version: None,
            schema,
            partition_spec: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
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
    #[allow(clippy::new_without_default)]
    pub fn new(
        commit_count: i32,
        op_append_count: i32,
        op_overwrite_count: i32,
        op_delete_count: i32,
        op_replace_count: i32,
        total_bytes: i32,
        bytes_added: i32,
        bytes_removed: i32,
        total_rows: i32,
        rows_added: i32,
        rows_deleted: i32,
    ) -> Statistics {
        Statistics {
            commit_count,
            op_append_count,
            op_overwrite_count,
            op_delete_count,
            op_replace_count,
            total_bytes,
            bytes_added,
            bytes_removed,
            total_rows,
            rows_added,
            rows_deleted,
            table_count: None,
            database_count: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteDefault(swagger::AnyOf6<String, bool, i32, f64, swagger::ByteArray, uuid::Uuid>);
