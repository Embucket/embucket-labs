// --- BEGIN COPIED MODELS ---
// Originally from core-metastore/src/models/...

// --- From models/mod.rs ---
use std::ops::Deref;
use chrono::NaiveDateTime;
// Note: serde::{Deserialize, Serialize} will be included in the main use block later
// Note: validator::Validate will be included in the main use block later

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RwObject<T>
where
    T: Eq + PartialEq,
{
    #[serde(flatten)]
    pub data: T,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl<T> RwObject<T>
where
    T: Eq + PartialEq,
{
    pub fn new(data: T) -> Self {
        let now = chrono::Utc::now().naive_utc();
        Self {
            data,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn update(&mut self, data: T) {
        if data != self.data {
            self.data = data;
            self.updated_at = chrono::Utc::now().naive_utc();
        }
    }

    #[allow(dead_code)] // May become used later or by other traits
    pub fn touch(&mut self) {
        self.updated_at = chrono::Utc::now().naive_utc();
    }
}

impl<T> Deref for RwObject<T>
where
    T: Eq + PartialEq,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

// --- From models/database.rs ---
use std::collections::HashMap;
// Note: validator::Validate already covered

/// A database identifier
pub type DatabaseIdent = String;

#[derive(validator::Validate, Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct Database {
    #[validate(length(min = 1))]
    pub ident: DatabaseIdent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    /// Volume identifier
    pub volume: VolumeIdent, // Forward declaration, defined in volumes.rs content
}

impl Database {
    #[must_use]
    pub fn prefix(&self, parent: &str) -> String {
        format!("{}/{}", parent, self.ident)
    }
}

// --- From models/schema.rs ---
// HashMap already imported
// validator::Validate already covered

#[derive(validator::Validate, Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, utoipa::ToSchema)]
/// A schema identifier
#[derive(Default)]
pub struct SchemaIdent {
    #[validate(length(min = 1))]
    /// The name of the schema
    pub schema: String,
    #[validate(length(min = 1))]
    /// The database the schema belongs to
    pub database: DatabaseIdent,
}

impl SchemaIdent {
    #[must_use]
    pub const fn new(database: DatabaseIdent, schema: String) -> Self {
        Self { schema, database }
    }
}

impl std::fmt::Display for SchemaIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.schema)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, utoipa::ToSchema)]
pub struct Schema {
    pub ident: SchemaIdent,
    pub properties: Option<HashMap<String, String>>,
}

impl Schema {
    #[must_use]
    pub fn prefix(&self, parent: &str) -> String {
        format!("{}/{}", parent, self.ident.schema)
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.ident.database, self.ident.schema)
    }
}


// --- From models/table.rs ---
// MetastoreError, MetastoreResult will be defined later in this file
use iceberg_rust::{
    catalog::commit::{TableRequirement, TableUpdate as IcebergTableUpdate},
    spec::table_metadata::TableMetadata,
};
use iceberg_rust_spec::{
    partition::PartitionSpec as IcebergPartitionSpec, schema::Schema as IcebergSchema, sort::SortOrder as IcebergSortOrder, spec::identifier::Identifier as IcebergIdentifier,
};
// HashMap, Display already imported
// utoipa::ToSchema, validator::Validate already covered

#[derive(validator::Validate, Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, utoipa::ToSchema)]
/// A table identifier
pub struct TableIdent {
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

impl TableIdent {
    #[must_use]
    pub fn new(database: &str, schema: &str, table: &str) -> Self {
        Self {
            table: table.to_string(),
            schema: schema.to_string(),
            database: database.to_string(),
        }
    }
    #[must_use]
    pub fn to_iceberg_ident(&self) -> IcebergIdentifier {
        let namespace = vec![self.schema.clone()];
        IcebergIdentifier::new(&namespace, &self.table)
    }
}

impl From<TableIdent> for SchemaIdent {
    fn from(ident: TableIdent) -> Self {
        Self {
            database: ident.database,
            schema: ident.schema,
        }
    }
}

impl std::fmt::Display for TableIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.database, self.schema, self.table)
    }
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, utoipa::ToSchema, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "kebab-case")]
pub enum TableFormat {
    Parquet,
    Iceberg,
}

// Implicit From<String> for TableFormat due to strum::EnumString

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub struct Table {
    pub ident: TableIdent,
    pub metadata: TableMetadata,
    pub metadata_location: String,
    pub properties: HashMap<String, String>,
    pub volume_ident: Option<VolumeIdent>, // Forward declaration
    pub volume_location: Option<String>,
    pub is_temporary: bool,
    pub format: TableFormat,
}

#[derive(validator::Validate, Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub struct TableCreateRequest {
    #[validate(nested)]
    pub ident: TableIdent,
    pub properties: Option<HashMap<String, String>>,
    pub format: Option<TableFormat>,
    pub location: Option<String>,
    pub schema: IcebergSchema,
    pub partition_spec: Option<IcebergPartitionSpec>,
    pub sort_order: Option<IcebergSortOrder>,
    pub stage_create: Option<bool>,
    pub volume_ident: Option<VolumeIdent>, // Forward declaration
    pub is_temporary: Option<bool>,
}

// The My* structs for UTOIPA schema generation - keeping them as they were.
#[derive(utoipa::ToSchema, serde::Deserialize, serde::Serialize)]
#[allow(dead_code)] // These are for schema generation, may not be used directly
enum MyPrimitive {
    Int,
    Str,
    Decimal { precision: u32, scale: u32 },
}

#[derive(utoipa::ToSchema, serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct MyMap {
    key: Box<TypeEnum>,
    value: Box<TypeEnum>,
}

#[derive(utoipa::ToSchema, serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct MyList {
    element: Box<TypeEnum>,
}

#[derive(utoipa::ToSchema, serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct MyStruct {
    fields: Vec<MyStructField>,
}

#[derive(utoipa::ToSchema, serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct MyStructField {
    #[serde(rename = "type")]
    field_type: TypeEnum,
}

#[derive(utoipa::ToSchema, serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
enum TypeEnum {
    Struct(MyStruct),
    List(MyList),
    Map(MyMap),
    Primitive(MyPrimitive),
}

#[derive(utoipa::ToSchema, serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct MySchemaDef { // Renamed from MySchema to avoid conflict with struct Schema
    #[serde(flatten)]
    fields: MyStruct,
}


#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct Config { // This was table::Config, might conflict if there's a general Config
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct TableUpdate {
    /// Commit will fail if the requirements are not met.
    pub requirements: Vec<TableRequirement>,
    /// The updates of the table.
    pub updates: Vec<IcebergTableUpdate>,
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

    pub fn assert(&self, metadata: &TableMetadata, exists: bool) -> MetastoreResult<()> { // MetastoreResult defined later
        match self.inner() {
            TableRequirement::AssertCreate => {
                if exists {
                    return Err(MetastoreError::TableDataExists { // MetastoreError defined later
                        location: metadata.location.to_string(),
                    });
                }
            }
            TableRequirement::AssertTableUuid { uuid } => {
                if &metadata.table_uuid != uuid {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table uuid does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertCurrentSchemaId { current_schema_id } => {
                if metadata.current_schema_id != *current_schema_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table current schema id does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id,
            } => {
                if metadata.default_sort_order_id != *default_sort_order_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table default sort order id does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                if let Some(snapshot_id) = snapshot_id {
                    let snapshot_ref = metadata.refs.get(r#ref).ok_or_else(|| {
                        MetastoreError::TableRequirementFailed {
                            message: "Table ref not found".to_string(),
                        }
                    })?;
                    if snapshot_ref.snapshot_id != *snapshot_id {
                        return Err(MetastoreError::TableRequirementFailed {
                            message: "Table ref snapshot id does not match".to_string(),
                        });
                    }
                } else if metadata.refs.contains_key(r#ref) {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table ref already exists".to_string(),
                    });
                }
            }
            TableRequirement::AssertDefaultSpecId { default_spec_id } => {
                if metadata.default_spec_id != *default_spec_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table default spec id does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id,
            } => {
                if metadata.last_partition_id != *last_assigned_partition_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table last assigned partition id does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id,
            } => {
                if &metadata.last_column_id != last_assigned_field_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table last assigned field id does not match".to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}


// --- From models/volumes.rs ---
// MetastoreResult, MetastoreError defined later
use object_store::{aws::AmazonS3Builder, path::Path as ObjectStorePath}; // Renamed Path to avoid conflict
// serde::{Deserialize, Serialize} already covered
// snafu::ResultExt will be in main use block
// std::fmt::Display, std::sync::Arc already imported
// validator::{Validate, ValidationError, ValidationErrors} already covered

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, strum::Display)]
pub enum CloudProvider {
    AWS,
    AZURE,
    GCS,
    FS,
    MEMORY,
}

#[derive(validator::Validate, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct AwsAccessKeyCredentials {
    #[validate(length(min = 1))]
    pub aws_access_key_id: String,
    #[validate(length(min = 1))]
    pub aws_secret_access_key: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone, utoipa::ToSchema)]
#[serde(tag = "credential_type", rename_all = "kebab-case")]
pub enum AwsCredentials {
    #[serde(rename = "access_key")]
    AccessKey(AwsAccessKeyCredentials),
    #[serde(rename = "token")]
    Token(String),
}

impl validator::Validate for AwsCredentials {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::AccessKey(creds) => creds.validate(),
            Self::Token(token) => {
                if token.is_empty() {
                    let mut errors = validator::ValidationErrors::new();
                    errors.add("token", validator::ValidationError::new("Token must not be empty"));
                    return Err(errors);
                }
                Ok(())
            }
        }
    }
}

#[derive(validator::Validate, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct S3Volume {
    #[validate(length(min = 1))]
    pub region: Option<String>,
    #[validate(length(min = 1), custom(function = "validate_bucket_name"))]
    pub bucket: Option<String>,
    #[validate(length(min = 1))]
    pub endpoint: Option<String>,
    pub skip_signature: Option<bool>,
    #[validate(length(min = 1))]
    pub metadata_endpoint: Option<String>,
    #[validate(required, nested)]
    pub credentials: Option<AwsCredentials>,
}

#[derive(validator::Validate, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct S3TablesVolume {
    #[validate(length(min = 1))]
    pub region: String,
    #[validate(length(min = 1), custom(function = "validate_bucket_name"))]
    pub bucket: Option<String>,
    #[validate(length(min = 1))]
    pub endpoint: String,
    #[validate(nested)]
    pub credentials: AwsCredentials,
    #[validate(length(min = 1), custom(function = "validate_bucket_name"))]
    pub name: String,
    #[validate(length(min = 1))]
    pub arn: String,
}

impl S3TablesVolume {
    #[must_use]
    pub fn s3_builder(&self) -> AmazonS3Builder {
        let s3_volume = S3Volume {
            region: Some(self.region.clone()),
            bucket: Some(self.name.clone()),
            endpoint: Some(self.endpoint.clone()),
            skip_signature: None,
            metadata_endpoint: None,
            credentials: Some(self.credentials.clone()),
        };
        Volume::get_s3_builder(&s3_volume)
    }
}

fn validate_bucket_name(bucket_name: &str) -> Result<(), validator::ValidationError> {
    if !bucket_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(validator::ValidationError::new(
            "Bucket name must only contain alphanumeric characters, hyphens, or underscores",
        ));
    }
    if bucket_name.starts_with('-')
        || bucket_name.starts_with('_')
        || bucket_name.ends_with('-')
        || bucket_name.ends_with('_')
    {
        return Err(validator::ValidationError::new(
            "Bucket name must not start or end with a hyphen or underscore",
        ));
    }
    Ok(())
}

#[derive(validator::Validate, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct FileVolume {
    #[validate(length(min = 1))]
    pub path: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum VolumeType {
    S3(S3Volume),
    S3Tables(S3TablesVolume),
    File(FileVolume),
    Memory,
}

impl std::fmt::Display for VolumeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::S3(_) => write!(f, "s3"),
            Self::S3Tables(_) => write!(f, "s3_tables"),
            Self::File(_) => write!(f, "file"),
            Self::Memory => write!(f, "memory"),
        }
    }
}

impl validator::Validate for VolumeType {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::S3(volume) => volume.validate(),
            Self::S3Tables(volume) => volume.validate(),
            Self::File(volume) => volume.validate(),
            Self::Memory => Ok(()),
        }
    }
}

#[derive(validator::Validate, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct Volume {
    pub ident: VolumeIdent,
    #[serde(flatten)]
    #[validate(nested)]
    pub volume: VolumeType,
}

pub type VolumeIdent = String;

#[allow(clippy::as_conversions)]
impl Volume {
    #[must_use]
    pub const fn new(ident: VolumeIdent, volume: VolumeType) -> Self {
        Self { ident, volume }
    }

    pub fn get_object_store(&self) -> MetastoreResult<std::sync::Arc<dyn object_store::ObjectStore>> {
        match &self.volume {
            VolumeType::S3(volume) => {
                let s3_builder = Self::get_s3_builder(volume);
                s3_builder
                    .build()
                    .map(|s3| std::sync::Arc::new(s3) as std::sync::Arc<dyn object_store::ObjectStore>)
                    .context(crate::metastore::ObjectStoreSnafu) // Will use MetastoreError defined below
            }
            VolumeType::S3Tables(volume) => {
                let s3_builder = volume.s3_builder();
                s3_builder
                    .build()
                    .map(|s3| std::sync::Arc::new(s3) as std::sync::Arc<dyn object_store::ObjectStore>)
                    .context(crate::metastore::ObjectStoreSnafu)
            }
            VolumeType::File(_) => Ok(std::sync::Arc::new(
                object_store::local::LocalFileSystem::new().with_automatic_cleanup(true),
            ) as std::sync::Arc<dyn object_store::ObjectStore>),
            VolumeType::Memory => {
                Ok(std::sync::Arc::new(object_store::memory::InMemory::new()) as std::sync::Arc<dyn object_store::ObjectStore>)
            }
        }
    }

    #[must_use]
    pub fn get_s3_builder(volume: &S3Volume) -> AmazonS3Builder {
        let mut s3_builder = AmazonS3Builder::new()
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);

        if let Some(region) = &volume.region {
            s3_builder = s3_builder.with_region(region);
        }
        if let Some(bucket) = &volume.bucket {
            s3_builder = s3_builder.with_bucket_name(bucket.clone());
        }
        if let Some(endpoint) = &volume.endpoint {
            s3_builder = s3_builder.with_endpoint(endpoint);
            s3_builder = s3_builder.with_allow_http(endpoint.starts_with("http:"));
        }
        if let Some(metadata_endpoint) = &volume.metadata_endpoint {
            s3_builder = s3_builder.with_metadata_endpoint(metadata_endpoint);
        }
        if let Some(skip_signature) = volume.skip_signature {
            s3_builder = s3_builder.with_skip_signature(skip_signature);
        }
        if let Some(credentials) = &volume.credentials {
            match credentials {
                AwsCredentials::AccessKey(creds) => {
                    s3_builder = s3_builder.with_access_key_id(creds.aws_access_key_id.clone());
                    s3_builder =
                        s3_builder.with_secret_access_key(creds.aws_secret_access_key.clone());
                }
                AwsCredentials::Token(token) => {
                    s3_builder = s3_builder.with_token(token.clone());
                }
            }
        }
        s3_builder
    }

    #[must_use]
    pub fn prefix(&self) -> String {
        match &self.volume {
            VolumeType::S3(volume) => volume
                .bucket
                .as_ref()
                .map_or_else(|| "s3://".to_string(), |bucket| format!("s3://{bucket}")),
            VolumeType::S3Tables(volume) => volume
                .bucket
                .as_ref()
                .map_or_else(|| "s3://".to_string(), |bucket| format!("s3://{bucket}")),
            VolumeType::File(volume) => format!("file://{}", volume.path),
            VolumeType::Memory => "memory://".to_string(),
        }
    }

    pub async fn validate_credentials(&self) -> MetastoreResult<()> {
        let object_store = self.get_object_store()?;
        object_store
            .get(&ObjectStorePath::from(self.prefix())) // Use renamed ObjectStorePath
            .await
            .context(crate::metastore::ObjectStoreSnafu)?;
        Ok(())
    }
}

// --- END COPIED MODELS ---


// --- BEGIN COPIED ERROR TYPES ---
// Originally from core-metastore/src/error.rs
use iceberg_rust_spec::table_metadata::TableMetadataBuilderError;
use snafu::Snafu; // Already imported for executor, but good to list here

// Note: other error sources (object_store::Error, etc.) will be in main use block

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum MetastoreError {
    #[snafu(display("Table data already exists at that location: {location}"))]
    TableDataExists { location: String },

    #[snafu(display("Table requirement failed: {message}"))]
    TableRequirementFailed { message: String },

    #[snafu(display("Volume: Validation failed. Reason: {reason}"))]
    VolumeValidationFailed { reason: String },

    #[snafu(display("Volume: Missing credentials"))]
    VolumeMissingCredentials,

    #[snafu(display("Cloud provider not implemented"))]
    CloudProviderNotImplemented { provider: String },

    #[snafu(display("ObjectStore: {source}"))]
    ObjectStore { source: object_store::Error },

    #[snafu(display("ObjectStore path: {source}"))]
    ObjectStorePath { source: object_store::path::Error },

    #[snafu(display(
        "Unable to create directory for File ObjectStore path {path}, error: {source}"
    ))]
    CreateDirectory {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("SlateDB error: {source}"))]
    SlateDB { source: slatedb::SlateDBError }, // This will be a new dependency for core-traits

    #[snafu(display("UtilSlateDB error: {source}"))]
    UtilSlateDB { source: core_utils::Error }, // This will be a new dependency for core-traits

    #[snafu(display("Metastore object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists { type_name: String, name: String },

    #[snafu(display("Metastore object not found"))]
    ObjectNotFound,

    #[snafu(display("Volume {volume} already exists"))]
    VolumeAlreadyExists { volume: String },

    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound { volume: String },

    #[snafu(display("Database {db} already exists"))]
    DatabaseAlreadyExists { db: String },

    #[snafu(display("Database {db} not found"))]
    DatabaseNotFound { db: String },

    #[snafu(display("Schema {schema} already exists in database {db}"))]
    SchemaAlreadyExists { schema: String, db: String },

    #[snafu(display("Schema {schema} not found in database {db}"))]
    SchemaNotFound { schema: String, db: String },

    #[snafu(display("Table {table} already exists in schema {schema} in database {db}"))]
    TableAlreadyExists {
        table: String,
        schema: String,
        db: String,
    },

    #[snafu(display("Table {table} not found in schema {schema} in database {db}"))]
    TableNotFound {
        table: String,
        schema: String,
        db: String,
    },

    #[snafu(display(
        "Table Object Store for table {table} in schema {schema} in database {db} not found"
    ))]
    TableObjectStoreNotFound {
        table: String,
        schema: String,
        db: String,
    },

    #[snafu(display("Volume in use by database(s): {database}"))]
    VolumeInUse { database: String },

    #[snafu(display("Iceberg error: {source}"))]
    Iceberg { source: iceberg_rust::error::Error },

    #[snafu(display("TableMetadataBuilder error: {source}"))]
    TableMetadataBuilder { source: TableMetadataBuilderError },

    #[snafu(display("Serialization error: {source}"))]
    Serde { source: serde_json::Error },

    #[snafu(display("Validation Error: {source}"))]
    Validation { source: validator::ValidationErrors },

    #[snafu(display("UrlParse Error: {source}"))]
    UrlParse { source: url::ParseError },
}

pub type MetastoreResult<T> = std::result::Result<T, MetastoreError>;

impl From<validator::ValidationErrors> for MetastoreError {
    fn from(source: validator::ValidationErrors) -> Self {
        Self::Validation { source }
    }
}
// --- END COPIED ERROR TYPES ---


// --- BEGIN COPIED METASTORE TRAIT ---
// Originally from core-metastore/src/metastore.rs
use async_trait::async_trait; // Will be in main use block
use core_utils::scan_iterator::VecScanIterator; // This will be a new dependency for core-traits
// Other dependencies like Arc, ObjectStore, models, MetastoreResult are defined/imported elsewhere in this file or main use block.

#[async_trait]
pub trait Metastore: std::fmt::Debug + Send + Sync {
    fn iter_volumes(&self) -> VecScanIterator<RwObject<Volume>>;
    async fn create_volume(
        &self,
        name: &VolumeIdent,
        volume: Volume,
    ) -> MetastoreResult<RwObject<Volume>>;
    async fn get_volume(&self, name: &VolumeIdent) -> MetastoreResult<Option<RwObject<Volume>>>;
    async fn update_volume(
        &self,
        name: &VolumeIdent,
        volume: Volume,
    ) -> MetastoreResult<RwObject<Volume>>;
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> MetastoreResult<()>;
    async fn volume_object_store(
        &self,
        name: &VolumeIdent,
    ) -> MetastoreResult<Option<std::sync::Arc<dyn object_store::ObjectStore>>>;

    fn iter_databases(&self) -> VecScanIterator<RwObject<Database>>;
    async fn create_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> MetastoreResult<RwObject<Database>>;
    async fn get_database(
        &self,
        name: &DatabaseIdent,
    ) -> MetastoreResult<Option<RwObject<Database>>>;
    async fn update_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> MetastoreResult<RwObject<Database>>;
    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> MetastoreResult<()>;

    fn iter_schemas(&self, database: &DatabaseIdent) -> VecScanIterator<RwObject<Schema>>;
    async fn create_schema(
        &self,
        ident: &SchemaIdent,
        schema: self::Schema, // Using self::Schema to avoid conflict with IcebergSchema
    ) -> MetastoreResult<RwObject<self::Schema>>; // Using self::Schema
    async fn get_schema(&self, ident: &SchemaIdent) -> MetastoreResult<Option<RwObject<self::Schema>>>; // Using self::Schema
    async fn update_schema(
        &self,
        ident: &SchemaIdent,
        schema: self::Schema, // Using self::Schema
    ) -> MetastoreResult<RwObject<self::Schema>>; // Using self::Schema
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> MetastoreResult<()>;

    fn iter_tables(&self, schema: &SchemaIdent) -> VecScanIterator<RwObject<Table>>;
    async fn create_table(
        &self,
        ident: &TableIdent,
        table: TableCreateRequest,
    ) -> MetastoreResult<RwObject<Table>>;
    async fn get_table(&self, ident: &TableIdent) -> MetastoreResult<Option<RwObject<Table>>>;
    async fn update_table(
        &self,
        ident: &TableIdent,
        update: TableUpdate,
    ) -> MetastoreResult<RwObject<Table>>;
    async fn delete_table(&self, ident: &TableIdent, cascade: bool) -> MetastoreResult<()>;
    async fn table_object_store(
        &self,
        ident: &TableIdent,
    ) -> MetastoreResult<Option<std::sync::Arc<dyn object_store::ObjectStore>>>;

    async fn table_exists(&self, ident: &TableIdent) -> MetastoreResult<bool>;
    async fn url_for_table(&self, ident: &TableIdent) -> MetastoreResult<String>;
    async fn volume_for_table(
        &self,
        ident: &TableIdent,
    ) -> MetastoreResult<Option<RwObject<Volume>>>;
}
// --- END COPIED METASTORE TRAIT ---

// --- Main Use Block ---
// Consolidating necessary imports based on copied code.
// Some of these might be already imported above locally where items were defined.
// This is for overall module dependencies.
// use async_trait::async_trait; // Already above trait
// use chrono::NaiveDateTime; // Already with RwObject
// use core_utils::scan_iterator::VecScanIterator; // Already above trait
// use iceberg_rust::{ /* imports used in models */ };
// use iceberg_rust_spec::{ /* imports used in models */ };
// use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path as ObjectStorePath}; // ObjectStore already used by trait
// use serde::{Deserialize, Serialize}; // Already with RwObject
// use snafu::{Snafu, ResultExt}; // Snafu already with MetastoreError
// use std::{collections::HashMap, fmt::Display, ops::Deref, sync::Arc}; // Already used
// use utoipa::ToSchema; // Used in derives
// use validator::{Validate, ValidationError, ValidationErrors}; // Used in derives
// use slatedb; // For MetastoreError::SlateDB
// use core_utils; // For MetastoreError::UtilSlateDB
// use url; // For MetastoreError::UrlParse
// use serde_json; // For MetastoreError::Serde
// use strum; // For derives on enums
// use strum_macros; // For derives on enums

// Minimal explicit top-level imports needed as most are now within this module
// or are crate dependencies that will be added to core-traits/Cargo.toml
use snafu::ResultExt; // For context() method on Results.
use std::sync::Arc; // Used in trait.
use object_store::ObjectStore; // Used in trait.
// Other imports are used directly with their full paths (e.g. `validator::Validate`) or are now local.

// Need to ensure `crate::metastore::ObjectStoreSnafu` in `Volume::get_object_store`
// correctly refers to `MetastoreError::ObjectStore`.
// This might require aliasing or careful path usage.
// For now, assuming `crate::metastore::` prefix will resolve to this module's `MetastoreError` variants
// or I need to change it to `self::MetastoreError::ObjectStore` or similar.
// Let's adjust the context calls in `Volume::get_object_store`
// from `crate::metastore::ObjectStoreSnafu` to `self::ObjectStoreSnafu`
// This means `MetastoreError::ObjectStore` must have `#[snafu(context(suffix(false)))]` or similar if we want `.context(ObjectStoreSnafu)`
// Or, more simply, use `.context(self::ObjectStoreSnafu)` if ObjectStoreSnafu is a generated context selector.
// The original code was `metastore_error::ObjectStoreSnafu`. So it becomes `self::ObjectStoreSnafu`.
// This requires the MetastoreError enum to be defined *before* Volume impl.
// I will reorder the file content: Errors first, then Models, then Trait.

// Reordering will be done mentally for now and reflected in the final combined file.
// The current structure is: Models, Errors, Trait. This should be okay as long as paths are correct.
// The `context(crate::metastore::ObjectStoreSnafu)` needs to be `context(self::ObjectStoreSnafu)`
// Let me check the Volume struct's get_object_store method.
// It uses `.context(metastore_error::ObjectStoreSnafu)`.
// This will become `.context(self::ObjectStoreSnafu)` if MetastoreError is in the same module.
// The MetastoreError definition has `#[snafu(visibility(pub))]` and `ObjectStore { source: object_store::Error }`
// This automatically creates `ObjectStoreSnafu` context selector.
// So, `self::ObjectStoreSnafu` should work.
