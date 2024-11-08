use crate::http::ui::models::table::Table;
use catalog::models::{TableCommit, TableIdent};
use iceberg::TableUpdate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableSettingsResponse {
    snapshots_management: SnapshotsManagement,
    automatic_compaction: AutomaticCompaction,
    lifecycle_policies: LifecyclePolicies,
    user_managed: UserManaged,
}

impl From<Table> for TableSettingsResponse {
    fn from(table: Table) -> Self {
        let mut response = TableSettingsResponse::default();
        for (id, value) in table.metadata.0.properties.iter() {
            match id.as_str() {
                "history.expire.max-snapshot-age-ms" => {
                    response.snapshots_management.max_snapshot_age_ms =
                        value.parse().ok();
                }
                "history.expire.min-snapshots-to-keep" => {
                    response.snapshots_management.min_snapshots_to_keep =
                        value.parse().ok();
                }
                "history.expire.max-ref-age-ms" => {
                    response.snapshots_management.max_ref_age_ms = value.parse().ok();
                }
                "compaction.enabled" => {
                    response.automatic_compaction.enabled = value.parse().ok();
                }
                "lifecycle.enabled" => {
                    response.lifecycle_policies.enabled = value.parse().ok();
                }
                _ => {
                    if id.starts_with("user_managed.") {
                        response.user_managed.items.push(Property {
                            id: id.clone(),
                            value: value.clone(),
                        });
                    }
                }
            }
        }
        response
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableUpdatePropertiesPayload {
    pub properties: Properties,
}

impl TableUpdatePropertiesPayload {
    pub(crate) fn to_commit(&self, table: Table, ident: &TableIdent) -> TableCommit {
        match &self.properties {
            Properties::SnapshotsManagement(p) => p.to_commit(ident),
            Properties::AutomaticCompaction(p) => p.to_commit(ident),
            Properties::LifecyclePolicies(p) => p.to_commit(ident),
            Properties::UserManaged(p) => p.to_commit(table, ident),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Properties {
    SnapshotsManagement(SnapshotsManagement),
    AutomaticCompaction(AutomaticCompaction),
    LifecyclePolicies(LifecyclePolicies),
    UserManaged(UserManaged),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
pub struct SnapshotsManagement {
    /// A positive number for the minimum number of snapshots to keep in a branch while expiring snapshots.
    /// Defaults to table property history.expire.min-snapshots-to-keep.
    #[serde(skip_serializing_if = "Option::is_none")]
    min_snapshots_to_keep: Option<i32>,
    /// A positive number for the max age of snapshots to keep when expiring, including the latest snapshot.
    /// Defaults to table property history.expire.max-snapshot-age-ms.
    #[serde(skip_serializing_if = "Option::is_none")]
    max_snapshot_age_ms: Option<i64>,
    /// For snapshot references except the main branch, a positive number for the max age of the snapshot reference to keep while expiring snapshots.
    /// Defaults to table property history.expire.max-ref-age-ms. The main branch never expires.
    #[serde(skip_serializing_if = "Option::is_none")]
    max_ref_age_ms: Option<i64>,
}

impl SnapshotsManagement {
    pub fn to_commit(&self, ident: &TableIdent) -> TableCommit {
        let mut properties = HashMap::new();
        let mut properties_to_remove = vec![];

        if let Some(min_snapshots_to_keep) = self.min_snapshots_to_keep {
            properties.insert(
                "history.expire.min-snapshots-to-keep".to_string(),
                min_snapshots_to_keep.to_string(),
            );
        } else {
            properties_to_remove.push("history.expire.min-snapshots-to-keep".to_string());
        }
        if let Some(max_snapshot_age_ms) = self.max_snapshot_age_ms {
            properties.insert(
                "history.expire.max-snapshot-age-ms".to_string(),
                max_snapshot_age_ms.to_string(),
            );
        } else {
            properties_to_remove.push("history.expire.max-snapshot-age-ms".to_string());
        }
        if let Some(max_ref_age_ms) = self.max_ref_age_ms {
            properties.insert(
                "history.expire.max-ref-age-ms".to_string(),
                max_ref_age_ms.to_string(),
            );
        } else {
            properties_to_remove.push("history.expire.max-ref-age-ms".to_string());
        }
        TableCommit {
            ident: ident.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::RemoveProperties {
                    removals: properties_to_remove,
                },
                TableUpdate::SetProperties {
                    updates: properties,
                },
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
pub struct AutomaticCompaction {
    enabled: Option<i32>,
}

impl AutomaticCompaction {
    fn to_commit(&self, ident: &TableIdent) -> TableCommit {
        let mut properties = HashMap::new();
        let mut properties_to_remove = vec![];
        if let Some(enabled) = self.enabled {
            properties.insert("compaction.enabled".to_string(), enabled.to_string());
        } else {
            properties_to_remove.push("compaction.enabled".to_string());
        }
        TableCommit {
            ident: ident.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::RemoveProperties {
                    removals: properties_to_remove,
                },
                TableUpdate::SetProperties {
                    updates: properties,
                },
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
pub struct LifecyclePolicies {
    enabled: Option<i32>,
}

impl LifecyclePolicies {
    fn to_commit(&self, ident: &TableIdent) -> TableCommit {
        let mut properties = HashMap::new();
        let mut properties_to_remove = vec![];
        if let Some(enabled) = self.enabled {
            properties.insert("lifecycle.enabled".to_string(), enabled.to_string());
        } else {
            properties_to_remove.push("lifecycle.enabled".to_string());
        }
        TableCommit {
            ident: ident.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::RemoveProperties {
                    removals: properties_to_remove,
                },
                TableUpdate::SetProperties {
                    updates: properties,
                },
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
pub struct UserManaged {
    items: Vec<Property>,
}

impl UserManaged {
    fn to_commit(&self, table: Table, ident: &TableIdent) -> TableCommit {
        // Find the properties that are managed by the user with prefix user_managed from the table
        let user_managed_prefix = "user_managed.";
        let properties_to_remove = table
            .properties
            .iter()
            .filter(|(k, v)| k.starts_with(user_managed_prefix))
            .map(|(k, _)| k.clone())
            .collect();

        let properties = self
            .items
            .iter()
            .map(|p| {
                let id = if p.id.starts_with(user_managed_prefix) {
                    p.id.clone()
                } else {
                    format!("{}{}", user_managed_prefix, p.id)
                };
                (id, p.value.clone())
            })
            .collect();

        TableCommit {
            ident: ident.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::RemoveProperties {
                    removals: properties_to_remove,
                },
                TableUpdate::SetProperties {
                    updates: properties,
                },
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
pub struct Property {
    id: String,
    value: String,
}
