use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use catalog::models::{Schema, SortOrder, TableCreation, TableMetadata, UnboundPartitionSpec};

// TODO: remove once this is defined in iceberg crate or made public in iceberg-catalog-rest
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TableSchema {
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    pub config: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateTableSchema {
    pub name: String,
    pub location: Option<String>,
    pub schema: Schema,
    pub partition_spec: Option<UnboundPartitionSpec>,
    pub write_order: Option<SortOrder>,
    pub stage_create: Option<bool>,
    pub properties: Option<std::collections::HashMap<String, String>>,
}

impl From<CreateTableSchema> for TableCreation {
    fn from(schema: CreateTableSchema) -> Self {
        TableCreation {
            name: schema.name,
            location: schema.location,
            schema: schema.schema,
            partition_spec: schema.partition_spec,
            sort_order: schema.write_order,
            properties: schema.properties.unwrap_or_default(),
        }
    }
}

impl From<TableMetadata> for TableSchema {
    fn from(metadata: TableMetadata) -> Self {
        TableSchema {
            metadata_location: None,
            metadata,
            config: None,
        }
    }
}

// // Following is due to we need somehow to derive ToSchema for external types
pub struct SortOrderDuplicate {
    /// Identifier for SortOrder, order_id `0` is no sort order.
    #[builder(default)]
    pub order_id: i64,
    /// Details of the sort
    #[builder(setter(each(name = "with_sort_field")), default)]
    pub fields: Vec<SortField>,
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
/// Entry for every column that is to be sorted
pub struct SortFieldDuplicate {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A transform that is used to produce values to be sorted on from the source column.
    pub transform: Transform,
    /// A sort direction, that can only be either asc or desc
    pub direction: SortDirection,
    /// A null order that describes the order of null values when sorted.
    pub null_order: NullOrder,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
/// Sort direction in a partition, either ascending or descending
pub enum SortDirectionDuplicate {
    /// Ascending
    #[serde(rename = "asc")]
    Ascending,
    /// Descending
    #[serde(rename = "desc")]
    Descending,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
/// Describes the order of null values when sorted.
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    /// Nulls are stored first
    First,
    #[serde(rename = "nulls-last")]
    /// Nulls are stored last
    Last,
}

// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
// pub struct TableMetadataDuplicate {
//     pub(crate) format_version: FormatVersion,
//     pub(crate) table_uuid: Uuid,
//     pub(crate) location: String,
//     pub(crate) last_sequence_number: i64,
//     pub(crate) last_updated_ms: i64,
//     pub(crate) last_column_id: i32,
//     pub(crate) schemas: HashMap<i32, SchemaRef>,
//     pub(crate) current_schema_id: i32,
//     pub(crate) partition_specs: HashMap<i32, PartitionSpecRef>,
//     pub(crate) default_spec_id: i32,
//     pub(crate) last_partition_id: i32,
//     pub(crate) properties: HashMap<String, String>,
//     pub(crate) current_snapshot_id: Option<i64>,
//     pub(crate) snapshots: HashMap<i64, SnapshotRef>,
//     pub(crate) snapshot_log: Vec<SnapshotLog>,
//     pub(crate) metadata_log: Vec<MetadataLog>,
//     pub(crate) sort_orders: HashMap<i64, SortOrderRef>,
//     pub(crate) default_sort_order_id: i64,
//     pub(crate) refs: HashMap<String, SnapshotReference>,
// }
