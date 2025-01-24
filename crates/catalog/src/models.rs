use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use uuid::Uuid;

pub use iceberg::{Namespace, NamespaceIdent, TableCreation, TableRequirement, TableUpdate};

pub use iceberg::spec::{
    NullOrder, PartitionSpec, Schema, Snapshot, SortDirection, SortField, SortOrder, TableMetadata,
    Transform, UnboundPartitionField, UnboundPartitionSpec, ViewMetadata, ViewVersion,
};
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, CatalogResult};

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Config {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TableCommit {
    /// The table ident.
    pub ident: TableIdent,
    /// The requirements of the table.
    ///
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

    pub fn assert(&self, metadata: &TableMetadata, exists: bool) -> CatalogResult<()> {
        match self.inner() {
            TableRequirement::NotExist => {
                if exists {
                    return Err(CatalogError::TableNotFound {
                        key: metadata.uuid().to_string(),
                    });
                }
            }
            TableRequirement::UuidMatch { uuid } => {
                if &metadata.uuid() != uuid {
                    return Err(CatalogError::TableRequirementFailed {
                        message: "Table uuid does not match".to_string(),
                    });
                }
            }
            TableRequirement::CurrentSchemaIdMatch { current_schema_id } => {
                // ToDo: Harmonize the types of current_schema_id
                if i64::from(metadata.current_schema_id) != *current_schema_id {
                    return Err(CatalogError::TableRequirementFailed {
                        message: "Table current schema id does not match".to_string(),
                    });
                }
            }
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id,
            } => {
                if metadata.default_sort_order_id != *default_sort_order_id {
                    return Err(CatalogError::TableRequirementFailed {
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
                            .ok_or(CatalogError::TableRequirementFailed {
                                message: "Table ref not found".to_string(),
                            })?;
                    if snapshot_ref.snapshot_id != *snapshot_id {
                        return Err(CatalogError::TableRequirementFailed {
                            message: "Table ref snapshot id does not match".to_string(),
                        });
                    }
                } else if metadata.refs.contains_key(r#ref) {
                    return Err(CatalogError::TableRequirementFailed {
                        message: "Table ref snapshot id does not match".to_string(),
                    });
                }
            }
            TableRequirement::DefaultSpecIdMatch { default_spec_id } => {
                // ToDo: Harmonize the types of default_spec_id
                if i64::from(metadata.default_partition_spec_id()) != *default_spec_id {
                    return Err(CatalogError::TableRequirementFailed {
                        message: "Table default spec id does not match".to_string(),
                    });
                }
            }
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id,
            } => {
                if i64::from(metadata.last_partition_id) != *last_assigned_partition_id {
                    return Err(CatalogError::TableRequirementFailed {
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
                    return Err(CatalogError::TableRequirementFailed {
                        message: "Table last assigned field id does not match".to_string(),
                    });
                }
            }
        };
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Table {
    pub ident: TableIdent,
    pub metadata: TableMetadata,
    pub metadata_location: String,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TableIdent {
    pub database: DatabaseIdent,
    pub table: String,
}

impl From<Table> for iceberg::TableIdent {
    fn from(table: Table) -> Self {
        Self {
            namespace: table.ident.database.namespace,
            name: table.ident.table,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WarehouseIdent(Uuid);

impl From<Uuid> for WarehouseIdent {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl WarehouseIdent {
    #[must_use]
    pub const fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }

    #[must_use]
    pub const fn id(&self) -> &Uuid {
        &self.0
    }
}

impl Display for WarehouseIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseIdent {
    pub namespace: NamespaceIdent,
    pub warehouse: WarehouseIdent,
}

impl Display for DatabaseIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.warehouse, self.namespace.join("."))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Database {
    pub ident: DatabaseIdent,
    pub properties: HashMap<String, String>,
}

impl Display for TableIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

impl From<Database> for NamespaceIdent {
    fn from(db: Database) -> Self {
        db.ident.namespace
    }
}
