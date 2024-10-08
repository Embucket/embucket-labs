use std::collections::HashMap;

pub use iceberg::{
    Namespace, NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate,
};

pub use iceberg::spec::{
    NullOrder, PartitionSpec, Schema, Snapshot, SortDirection, SortField, SortOrder, TableMetadata,
    Transform, UnboundPartitionField, UnboundPartitionSpec, ViewMetadata, ViewVersion,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Default)]
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
