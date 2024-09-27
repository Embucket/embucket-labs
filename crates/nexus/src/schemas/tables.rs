use serde::{Deserialize, Serialize};

use catalog::models::{
    Schema,
    SortOrder,
    TableMetadata,
    UnboundPartitionSpec,
    TableCreation,
};


// TODO: remove once this is defined in iceberg crate or made public in iceberg-catalog-rest
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    pub config: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
