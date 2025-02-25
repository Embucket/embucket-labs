use std::{collections::HashMap, fmt::Display};

use chrono::Utc;
use iceberg::{spec::TableMetadata, TableRequirement, TableUpdate};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use validator::{Validate, ValidationError, ValidationErrors};

pub use iceberg::spec::{
    NullOrder, PartitionSpec, Schema, Snapshot, SortDirection, SortField, SortOrder,
    Transform, UnboundPartitionField, UnboundPartitionSpec, ViewMetadata, ViewVersion,
};

use crate::error::{self as metastore_error, MetastoreResult, MetastoreError};

use super::IceBucketSchemaIdent;

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// A table identifier
pub struct IceBucketTableIdent {
    #[validate(length(min = 1))]
    /// The name of the table
    pub table: String,
    #[validate(length(min = 1))]
    /// The schema the table belongs to
    pub schema: String,
    #[validate(length(min = 1))]
    /// The database the table belongs to
    pub database: String,
}

impl From<IceBucketTableIdent> for IceBucketSchemaIdent {
    fn from(ident: IceBucketTableIdent) -> Self {
        IceBucketSchemaIdent {
            database: ident.database,
            schema: ident.schema,
        }
    }
}

impl Display for IceBucketTableIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.database, self.schema, self.table)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum IceBucketTableFormat {
    /*Parquet,
    Avro,
    Orc,
    Delta,
    Json,
    Csv,*/
    Iceberg,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct IceBucketTable {
    pub ident: IceBucketTableIdent,
    pub metadata: TableMetadata,
    pub metadata_location: String,
    pub properties: HashMap<String, String>,
}

#[derive(Validate, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct IceBucketTableCreateRequest {
    #[validate(nested)]
    pub ident: IceBucketTableIdent,
    pub properties: Option<HashMap<String, String>>,
    pub format: Option<IceBucketTableFormat>,

    pub location: Option<String>,
    pub schema: Schema,
    pub partition_spec: Option<UnboundPartitionSpec>,
    pub write_order: Option<SortOrder>,
    pub stage_create: Option<bool>,
}

impl From<IceBucketTableCreateRequest> for iceberg::TableCreation {
    fn from(schema: IceBucketTableCreateRequest) -> Self {
        let mut properties = schema.properties.unwrap_or_default();
        let utc_now = Utc::now();
        let utc_now_str = utc_now.to_rfc3339();
        properties.insert("created_at".to_string(), utc_now_str.clone());
        properties.insert("updated_at".to_string(), utc_now_str);

        Self {
            name: schema.ident.table,
            location: schema.location,
            schema: schema.schema,
            partition_spec: schema.partition_spec.map(std::convert::Into::into),
            sort_order: schema.write_order,
            properties,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Config {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IceBucketTableUpdate {
    /// Commit will fail if the requirements are not met.
    pub requirements: Vec<TableRequirement>,
    /// The updates of the table.
    pub updates: Vec<TableUpdate>,
}

pub struct TableRequirementExt(TableRequirement);

impl From<TableRequirement> for TableRequirementExt {
    fn from(requirement: TableRequirement) -> Self {
        Self(requirement)
    }
}

impl TableRequirementExt {
    #[must_use]
    pub const fn new(requirement: TableRequirement) -> Self {
        Self(requirement)
    }

    #[must_use]
    pub const fn inner(&self) -> &TableRequirement {
        &self.0
    }

    pub fn assert(&self, metadata: &TableMetadata, exists: bool) -> MetastoreResult<()> {
        match self.inner() {
            TableRequirement::NotExist => {
                if exists {
                    return Err(MetastoreError::TableDataExists {
                        location: metadata.location().to_string(),
                    });
                }
            }
            TableRequirement::UuidMatch { uuid } => {
                if &metadata.uuid() != uuid {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table uuid does not match".to_string(),
                    });
                }
            }
            TableRequirement::CurrentSchemaIdMatch { current_schema_id } => {
                // ToDo: Harmonize the types of current_schema_id
                if i64::from(metadata.current_schema_id) != *current_schema_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table current schema id does not match".to_string(),
                    });
                }
            }
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id,
            } => {
                if metadata.default_sort_order_id != *default_sort_order_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table default sort order id does not match".to_string(),
                    });
                }
            }
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                if let Some(snapshot_id) = snapshot_id {
                    let snapshot_ref =
                        metadata
                            .refs
                            .get(r#ref)
                            .ok_or_else(|| MetastoreError::TableRequirementFailed {
                                message: "Table ref not found".to_string(),
                            })?;
                    if snapshot_ref.snapshot_id != *snapshot_id {
                        return Err(MetastoreError::TableRequirementFailed {
                            message: "Table ref snapshot id does not match".to_string(),
                        });
                    }
                } else if metadata.refs.contains_key(r#ref) {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table ref snapshot id does not match".to_string(),
                    });
                }
            }
            TableRequirement::DefaultSpecIdMatch { default_spec_id } => {
                // ToDo: Harmonize the types of default_spec_id
                if i64::from(metadata.default_partition_spec_id()) != *default_spec_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table default spec id does not match".to_string(),
                    });
                }
            }
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id,
            } => {
                if i64::from(metadata.last_partition_id) != *last_assigned_partition_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table last assigned partition id does not match".to_string(),
                    });
                }
            }
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id,
            } => {
                // ToDo: Harmonize types
                let last_column_id: i64 = metadata.last_column_id.into();
                if &last_column_id != last_assigned_field_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table last assigned field id does not match".to_string(),
                    });
                }
            }
        };
        Ok(())
    }
}

// TODO: Eventually move table schema information into the catalog
// Ostensibly the catalog *should* be aware of the schema of the tables it contains
// However, the current implementation of the catalog does not store schema information
