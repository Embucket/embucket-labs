use iceberg::spec::TableMetadata;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::openapi::{ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMetadataWrapper(pub TableMetadata);

impl PartialSchema for TableMetadataWrapper {
    fn schema() -> RefOr<Schema> {
        RefOr::from(Schema::Object(
            ObjectBuilder::new()
                .property("format_version", i32::schema())
                .property("table_uuid", String::schema())
                .property("location", String::schema())
                .property("last_sequence_number", i64::schema())
                .property("last_updated_ms", i64::schema())
                .property("last_column_id", i32::schema())
                .property("schemas", Schema::Array(Default::default()))
                .property("current_schema_id", i32::schema())
                .property("partition_specs", Schema::Array(Default::default()))
                .property("default_spec", Schema::Object(Default::default()))
                .property("last_partition_id", i32::schema())
                .property("properties", HashMap::<String, String>::schema())
                .property("current_snapshot_id", Option::<i64>::schema())
                .property("snapshots", Schema::Array(Default::default()))
                .property("snapshot_log", Schema::Array(Default::default()))
                .property("metadata_log", Schema::Array(Default::default()))
                .property("sort_orders", Schema::Array(Default::default()))
                .property("default_sort_order_id", i32::schema())
                .property("refs", Schema::Object(Default::default()))
                .build(),
        ))
    }
}

impl ToSchema for TableMetadataWrapper {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("TableMetadataWrapper")
    }
}
