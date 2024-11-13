use crate::http::ui::models::table::Table;
use catalog::models::{TableCommit, TableIdent};
use chrono::{DateTime, Utc};
use iceberg::spec::Operation;
use iceberg::TableUpdate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableSnapshotsResponse {
    snapshots: Vec<TableSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableSnapshot {
    pub timestamp: DateTime<Utc>,
    pub operation: String,
    pub total_records: i32,
    pub added_records: i32,
    pub deleted_records: i32,
    pub snapshot_id: i64,
}

impl From<Table> for TableSnapshotsResponse {
    fn from(table: Table) -> Self {
        let mut snapshots = vec![];
        // Sort the snapshots by timestamp

        for (_, snapshot) in table.metadata.0.snapshots {
            let operation = match snapshot.summary().operation {
                Operation::Append => "append",
                Operation::Overwrite => "overwrite",
                Operation::Replace => "replace",
                Operation::Delete => "delete",
            };
            snapshots.push(TableSnapshot {
                timestamp: snapshot.timestamp().unwrap(),
                operation: operation.to_string(),
                total_records: snapshot
                    .summary()
                    .other
                    .get("total-records")
                    .and_then(|value| value.parse::<i32>().ok())
                    .unwrap_or(0),
                added_records: snapshot
                    .summary()
                    .other
                    .get("added-records")
                    .and_then(|value| value.parse::<i32>().ok())
                    .unwrap_or(0),
                deleted_records: snapshot
                    .summary()
                    .other
                    .get("deleted-records")
                    .and_then(|value| value.parse::<i32>().ok())
                    .unwrap_or(0),
                snapshot_id: snapshot.snapshot_id(),
            });
        }
        snapshots.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        TableSnapshotsResponse { snapshots }
    }
}

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
                    response.snapshots_management.max_snapshot_age_ms = value.parse().ok();
                }
                "history.expire.min-snapshots-to-keep" => {
                    response.snapshots_management.min_snapshots_to_keep = value.parse().ok();
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
                "lifecycle.max-data-age-ms" => {
                    response.lifecycle_policies.max_data_age_ms = value.parse().ok();
                }
                "lifecycle.data-age-column" => {
                    response.lifecycle_policies.data_age_column = Some(value.clone());
                }
                _ => {
                    if id.starts_with("user_managed.") {
                        response.user_managed.items.push(Property {
                            id: id.clone(),
                            value: value.clone(),
                        });
                    }
                    if id.starts_with("lifecycle.columns.") && id.ends_with(".max-data-age-ms") {
                        let column_name = id
                            .strip_prefix("lifecycle.columns.")
                            .unwrap()
                            .split('.')
                            .next()
                            .unwrap()
                            .to_string();
                        let transform = match table
                            .metadata
                            .0
                            .properties
                            .get(&format!("lifecycle.columns.{}.transform", column_name))
                            .unwrap()
                            .as_str()
                        {
                            "nullify" => ColumnTransform::Nullify,
                            _ => ColumnTransform::Nullify,
                        };
                        let column_policy = ColumnLevelPolicy {
                            column_name,
                            max_data_age_ms: value.parse().unwrap(),
                            transform,
                        };
                        response
                            .lifecycle_policies
                            .columns_max_data_age_ms
                            .get_or_insert_with(Vec::new)
                            .push(column_policy);
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
            Properties::LifecyclePolicies(p) => p.to_commit(ident, table.metadata.0.properties),
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct AutomaticCompaction {
    enabled: Option<bool>,
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
#[serde(rename_all = "camelCase")]
pub struct LifecyclePolicies {
    enabled: Option<bool>,
    // This column indicates when a given record originated
    data_age_column: Option<String>,
    // Row level policy
    max_data_age_ms: Option<i64>,
    // Column level policy
    columns_max_data_age_ms: Option<Vec<ColumnLevelPolicy>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ColumnLevelPolicy {
    column_name: String,
    max_data_age_ms: i64,
    transform: ColumnTransform,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum ColumnTransform {
    Nullify,
}

impl LifecyclePolicies {
    fn to_commit(&self, ident: &TableIdent, properties: HashMap<String, String>) -> TableCommit {
        let mut properties_to_add = HashMap::new();
        let mut properties_to_remove = vec![];

        for (k, _) in properties.iter() {
            if k.starts_with("lifecycle.") {
                properties_to_remove.push(k.clone());
            }
        }

        if let Some(enabled) = self.enabled {
            properties_to_add.insert("lifecycle.enabled".to_string(), enabled.to_string());
            if let Some(data_age_column) = &self.data_age_column {
                properties_to_add.insert(
                    "lifecycle.data-age-column".to_string(),
                    data_age_column.clone(),
                );
            }

            if let Some(max_data_age_ms) = self.max_data_age_ms {
                properties_to_add.insert(
                    "lifecycle.max-data-age-ms".to_string(),
                    max_data_age_ms.to_string(),
                );
            }

            if let Some(columns_max_data_age_ms) = &self.columns_max_data_age_ms {
                for column_policy in columns_max_data_age_ms {
                    properties_to_add.insert(
                        format!(
                            "lifecycle.columns.{}.max-data-age-ms",
                            column_policy.column_name
                        ),
                        column_policy.max_data_age_ms.to_string(),
                    );
                    properties_to_add.insert(
                        format!("lifecycle.columns.{}.transform", column_policy.column_name),
                        match column_policy.transform {
                            ColumnTransform::Nullify => "nullify".to_string(),
                        },
                    );
                }
            }
        }
        TableCommit {
            ident: ident.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::RemoveProperties {
                    removals: properties_to_remove,
                },
                TableUpdate::SetProperties {
                    updates: properties_to_add,
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
            .metadata
            .0
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
