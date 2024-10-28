use crate::http::ui::models::database::{CompactionSummary, Database, DatabaseExtended};
use crate::http::ui::models::metadata::TableMetadataWrapper;
use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::warehouse::Warehouse;
use catalog::models as CatalogModels;
use iceberg::spec::{Schema, SortOrder, UnboundPartitionSpec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::openapi::{ArrayBuilder, ObjectBuilder, RefOr, Schema as OpenApiSchema};
use utoipa::{PartialSchema, ToSchema};
use uuid::Uuid;
use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct TableCreateRequest {
    pub name: String,
    pub location: Option<String>,
    pub schema: SchemaWrapper,
    pub partition_spec: Option<UnboundPartitionSpecWrapper>,
    pub write_order: Option<SortOrderWrapper>,
    pub stage_create: Option<bool>,
    pub properties: Option<HashMap<String, String>>,
}

impl From<TableCreateRequest> for catalog::models::TableCreation {
    fn from(schema: TableCreateRequest) -> Self {
        catalog::models::TableCreation {
            name: schema.name,
            location: schema.location,
            schema: schema.schema.0,
            partition_spec: schema.partition_spec.map(|x| x.0),
            sort_order: schema.write_order.map(|x| x.0),
            properties: schema.properties.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
pub struct Table {
    pub id: Uuid,
    pub name: String,
    pub metadata: TableMetadataWrapper,
    pub metadata_location: String,
}

impl From<catalog::models::Table> for Table {
    fn from(table: catalog::models::Table) -> Self {
        Self {
            id: get_table_id(table.clone().ident),
            name: table.ident.table,
            metadata_location: table.metadata_location,
            metadata: TableMetadataWrapper(table.metadata),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct TableShort {
    pub id: Uuid,
    pub name: String,
}
pub fn get_table_id(ident: CatalogModels::TableIdent) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, ident.table.to_string().as_bytes())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct TableEntity {
    pub id: Uuid,
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
        id: Uuid,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TableExtended {
    pub id: Uuid,
    pub name: String,
    pub database_name: String,
    pub warehouse_id: Uuid,
    pub properties: Option<HashMap<String, String>>,
    pub metadata: TableMetadataWrapper,
    pub metadata_location: String,
    pub statistics: Option<Statistics>,
    pub compaction_summary: Option<CompactionSummary>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub database: DatabaseExtended,
}

impl TableExtended {
    #[allow(clippy::new_without_default)]
    pub fn new(
        profile: StorageProfile,
        warehouse: Warehouse,
        database: Database,
        table: CatalogModels::Table,
    ) -> TableExtended {
        TableExtended {
            id: get_table_id(table.clone().ident),
            name: table.ident.table,
            database_name: table.ident.database.to_string(),
            warehouse_id: warehouse.id,
            properties: None,
            metadata: TableMetadataWrapper(table.metadata),
            metadata_location: table.metadata_location,
            statistics: None,
            compaction_summary: None,
            created_at: Default::default(),
            updated_at: Default::default(),
            database: DatabaseExtended::new(profile, warehouse, database),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate, ToSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
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
