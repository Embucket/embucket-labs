use crate::http::utils::update_properties_timestamps;
pub use catalog::models::Config;
use catalog::models::Table;
use iceberg::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};
pub use iceberg::NamespaceIdent;
pub use iceberg::{TableIdent, TableRequirement, TableUpdate};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Namespace {
    /// Reference to one or more levels of a namespace
    pub namespace: NamespaceIdent,
    /// Configured string to string map of properties for the namespace
    pub properties: Option<std::collections::HashMap<String, String>>,
}

impl From<catalog::models::Database> for Namespace {
    fn from(db: catalog::models::Database) -> Self {
        Self {
            namespace: db.ident.namespace,
            properties: Some(db.properties),
        }
    }
}

/// Result used when a table is successfully loaded.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableResult {
    /// May be null if the table is staged as part of a transaction
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    pub config: Option<HashMap<String, String>>,
}

impl From<Table> for TableResult {
    fn from(table: Table) -> Self {
        Self {
            metadata_location: Some(table.metadata_location),
            metadata: table.metadata,
            config: Some(HashMap::default()), // FIXME: populate this
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableCreateRequest {
    pub name: String,
    pub location: Option<String>,
    pub schema: Schema,
    pub partition_spec: Option<UnboundPartitionSpec>,
    pub write_order: Option<SortOrder>,
    pub stage_create: Option<bool>,
    pub properties: Option<HashMap<String, String>>,
}

impl From<TableCreateRequest> for catalog::models::TableCreation {
    fn from(schema: TableCreateRequest) -> Self {
        let mut properties = schema.properties.unwrap_or_default();
        update_properties_timestamps(&mut properties);

        catalog::models::TableCreation {
            name: schema.name,
            location: schema.location,
            schema: schema.schema,
            partition_spec: schema.partition_spec.map(std::convert::Into::into),
            sort_order: schema.write_order,
            properties,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableRegisterRequest {
    pub name: String,
    pub metadata_location: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableListResponse {
    pub identifiers: Vec<TableIdent>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NamespaceListResponse {
    pub namespaces: Vec<NamespaceIdent>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TableCommitRequest {
    pub identifier: Option<TableIdent>,
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
    pub properties: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableCommitResponse {
    pub metadata_location: String,
    pub metadata: TableMetadata,
    pub config: Option<HashMap<String, String>>,
}

#[derive(serde::Deserialize)]
pub struct GetConfigQueryParams {
    pub warehouse: Option<Uuid>,
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ReportMetricsRequest {
    pub report_type: String,
    #[serde(flatten)]
    pub report: Report,
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Report {
    ScanReport(ScanReport),
    CommitReport(CommitReport),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanReport {
    pub table_name: String,
    pub snapshot_id: i64,
    pub filter: Expression,
    pub schema_id: i32,
    pub projected_field_ids: Vec<i32>,
    pub projected_field_names: Vec<String>,
    pub metrics: Metrics,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitReport {
    pub table_name: String,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub operation: String,
    pub metrics: Metrics,
    pub metadata: Option<HashMap<String, String>>,
}

// Assuming Expression and Metrics are defined elsewhere in your code
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Expression {
    // Define fields for Expression
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Metrics {
    // Define fields for Metrics
}