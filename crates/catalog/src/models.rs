use std::collections::HashMap;

pub use iceberg::{
    Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
};

pub use iceberg::spec::{
    NullOrder, PartitionSpec, Schema, Snapshot, SortDirection, SortField, SortOrder, TableMetadata,
    Transform, UnboundPartitionField, UnboundPartitionSpec, ViewMetadata, ViewVersion,
};

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Config {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
}
