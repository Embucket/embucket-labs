use std::collections::HashMap;

pub use iceberg::{
    NamespaceIdent,
    Namespace,
    TableIdent,
    TableCreation,
    TableCommit,
    TableRequirement,
    TableUpdate,
};

pub use iceberg::spec::{
    NullOrder, PartitionSpec, Schema, Snapshot, SortDirection, SortField, SortOrder, TableMetadata,
    UnboundPartitionField, UnboundPartitionSpec, ViewMetadata, ViewVersion,
};

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Config {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
}