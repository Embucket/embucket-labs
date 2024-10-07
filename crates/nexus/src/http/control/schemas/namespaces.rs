use std::ops::Deref;

pub use iceberg::NamespaceIdent;
use serde::{Deserialize, Serialize};
use utoipa::{openapi::schema, ToSchema};

use catalog::models::Namespace;

// TODO: Replace all the following with ones from iceberg OR iceberg-catalog-rest
// iceberg doesn't implement all while iceberg-catalog-rest has all as private

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NamespaceSchema {
    /// Reference to one or more levels of a namespace
    pub namespace: NamespaceIdent,
    /// Configured string to string map of properties for the namespace
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[schema(as = NamespaceIdent)]
pub struct NamespaceIdentDuplicate(Vec<String>);

impl From<Namespace> for NamespaceSchema {
    fn from(namespace: Namespace) -> Self {
        NamespaceSchema {
            namespace: namespace.name().clone(),
            properties: Some(namespace.properties().clone()),
        }
    }
}
