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

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Config {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
}