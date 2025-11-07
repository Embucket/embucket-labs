use std::{collections::HashMap, sync::Arc};

use crate::error::{self as metastore_error, Result};
use crate::models::{
    RwObject,
    database::{Database, DatabaseIdent},
    schema::{Schema, SchemaIdent},
    table::{Table, TableCreateRequest, TableFormat, TableIdent, TableRequirementExt, TableUpdate},
    volumes::{Volume, VolumeIdent},
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use iceberg_rust::catalog::commit::apply_table_updates;
use iceberg_rust_spec::{
    schema::Schema as IcebergSchema,
    table_metadata::{FormatVersion, TableMetadataBuilder},
    types::{StructField, Type},
};
use object_store::{ObjectStore, PutPayload, path::Path};
use snafu::ResultExt;
use tokio::sync::RwLock;
use tracing::instrument;
use uuid::Uuid;

#[async_trait]
pub trait Metastore: std::fmt::Debug + Send + Sync {
    async fn list_volumes(&self) -> Result<Vec<RwObject<Volume>>>;
    async fn create_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>>;
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>>;
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>>;
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()>;
    async fn volume_object_store(&self, name: &VolumeIdent)
    -> Result<Option<Arc<dyn ObjectStore>>>;

    async fn list_databases(&self) -> Result<Vec<RwObject<Database>>>;
    async fn create_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>>;
    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>>;
    async fn update_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>>;
    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> Result<()>;

    async fn list_schemas(&self, database: &DatabaseIdent) -> Result<Vec<RwObject<Schema>>>;
    async fn create_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>>;
    async fn get_schema(&self, ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>>;
    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>>;
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> Result<()>;

    async fn list_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>>;
    async fn create_table(
        &self,
        ident: &TableIdent,
        table: TableCreateRequest,
    ) -> Result<RwObject<Table>>;
    async fn get_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Table>>>;
    async fn update_table(
        &self,
        ident: &TableIdent,
        update: TableUpdate,
    ) -> Result<RwObject<Table>>;
    async fn delete_table(&self, ident: &TableIdent, cascade: bool) -> Result<()>;
    async fn table_object_store(&self, ident: &TableIdent) -> Result<Option<Arc<dyn ObjectStore>>>;

    async fn table_exists(&self, ident: &TableIdent) -> Result<bool>;
    async fn url_for_table(&self, ident: &TableIdent) -> Result<String>;
    async fn volume_for_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Volume>>>;
}

#[derive(Debug, Default)]
struct MetastoreState {
    volumes: HashMap<VolumeIdent, RwObject<Volume>>,
    databases: HashMap<DatabaseIdent, RwObject<Database>>,
    schemas: HashMap<(DatabaseIdent, String), RwObject<Schema>>,
    tables: HashMap<(DatabaseIdent, String, String), RwObject<Table>>,
}

#[derive(Debug)]
pub struct InMemoryMetastore {
    state: RwLock<MetastoreState>,
    object_store_cache: DashMap<VolumeIdent, Arc<dyn ObjectStore>>,
}

impl InMemoryMetastore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(MetastoreState::default()),
            object_store_cache: DashMap::new(),
        }
    }

    fn metadata_file_name() -> String {
        format!("{}.metadata.json", Uuid::new_v4())
    }

    fn metadata_path(table: &TableIdent, file_name: &str) -> String {
        format!(
            "{}/{}/{}",
            table.database.to_ascii_lowercase(),
            table.schema.to_ascii_lowercase(),
            file_name
        )
    }

    fn table_key(ident: &TableIdent) -> (DatabaseIdent, String, String) {
        (
            ident.database.to_ascii_lowercase(),
            ident.schema.to_ascii_lowercase(),
            ident.table.to_ascii_lowercase(),
        )
    }

    fn schema_key(ident: &SchemaIdent) -> (DatabaseIdent, String) {
        (
            ident.database.to_ascii_lowercase(),
            ident.schema.to_ascii_lowercase(),
        )
    }

    async fn ensure_volume(state: &MetastoreState, name: &VolumeIdent) -> Result<RwObject<Volume>> {
        state.volumes.get(name).cloned().ok_or_else(|| {
            metastore_error::VolumeNotFoundSnafu {
                volume: name.clone(),
            }
            .build()
        })
    }

    async fn put_metadata(
        &self,
        table: &TableIdent,
        object_store: Arc<dyn ObjectStore>,
        metadata: &iceberg_rust_spec::table_metadata::TableMetadata,
    ) -> Result<String> {
        let name = Self::metadata_file_name();
        let path = Self::metadata_path(table, &name);
        let bytes =
            serde_json::to_vec(metadata).context(metastore_error::SerializeMetadataSnafu)?;
        object_store
            .put(
                &Path::from(path.clone()),
                PutPayload::from_bytes(Bytes::from(bytes)),
            )
            .await
            .context(metastore_error::ObjectStoreSnafu)?;
        Ok(path)
    }
}

#[async_trait]
impl Metastore for InMemoryMetastore {
    async fn list_volumes(&self) -> Result<Vec<RwObject<Volume>>> {
        let state = self.state.read().await;
        Ok(state.volumes.values().cloned().collect())
    }

    #[instrument(
        name = "Metastore::create_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn create_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        let object_store = volume.get_object_store()?;
        let mut state = self.state.write().await;
        if state.volumes.contains_key(name) {
            return metastore_error::VolumeAlreadyExistsSnafu {
                volume: name.clone(),
            }
            .fail();
        }
        let row = RwObject::new(volume);
        state.volumes.insert(name.clone(), row.clone());
        self.object_store_cache.insert(name.clone(), object_store);
        Ok(row)
    }

    #[instrument(name = "Metastore::get_volume", level = "trace", skip(self), err)]
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>> {
        let state = self.state.read().await;
        Ok(state.volumes.get(name).cloned())
    }

    #[instrument(
        name = "Metastore::update_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        let object_store = volume.get_object_store()?;
        let mut state = self.state.write().await;
        let entry = state.volumes.get_mut(name).ok_or_else(|| {
            metastore_error::VolumeNotFoundSnafu {
                volume: name.clone(),
            }
            .build()
        })?;
        entry.update(volume);
        self.object_store_cache
            .insert(name.clone(), object_store.clone());
        Ok(entry.clone())
    }

    #[instrument(name = "Metastore::delete_volume", level = "debug", skip(self), err)]
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()> {
        let mut state = self.state.write().await;
        let in_use = state
            .databases
            .values()
            .filter(|db| &db.volume == name)
            .map(|db| db.ident.clone())
            .collect::<Vec<_>>();
        if !in_use.is_empty() && !cascade {
            return metastore_error::VolumeInUseSnafu {
                database: in_use.join(", "),
            }
            .fail();
        }
        if cascade {
            let schema_keys: Vec<_> = state
                .schemas
                .keys()
                .filter(|(db, _)| in_use.iter().any(|d| d == db))
                .cloned()
                .collect();
            for key in schema_keys {
                state.schemas.remove(&key);
            }
            let table_keys: Vec<_> = state
                .tables
                .keys()
                .filter(|(db, _, _)| in_use.iter().any(|d| d == db))
                .cloned()
                .collect();
            for key in table_keys {
                state.tables.remove(&key);
            }
            for db in in_use {
                state.databases.remove(&db);
            }
        } else if !in_use.is_empty() {
            return metastore_error::VolumeInUseSnafu {
                database: in_use.join(", "),
            }
            .fail();
        }
        if state.volumes.remove(name).is_some() {
            self.object_store_cache.remove(name);
        }
        Ok(())
    }

    async fn volume_object_store(
        &self,
        name: &VolumeIdent,
    ) -> Result<Option<Arc<dyn ObjectStore>>> {
        if let Some(store) = self.object_store_cache.get(name) {
            return Ok(Some(store.clone()));
        }
        let volume = self.get_volume(name).await?;
        if let Some(volume) = volume {
            let object_store = volume.get_object_store()?;
            self.object_store_cache
                .insert(name.clone(), object_store.clone());
            Ok(Some(object_store))
        } else {
            Ok(None)
        }
    }

    async fn list_databases(&self) -> Result<Vec<RwObject<Database>>> {
        let state = self.state.read().await;
        Ok(state.databases.values().cloned().collect())
    }

    #[instrument(
        name = "Metastore::create_database",
        level = "debug",
        skip(self, database),
        err
    )]
    async fn create_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>> {
        let mut state = self.state.write().await;
        Self::ensure_volume(&state, &database.volume).await?;
        if state.databases.contains_key(name) {
            return metastore_error::DatabaseAlreadyExistsSnafu { db: name }.fail();
        }
        let row = RwObject::new(database);
        state.databases.insert(name.clone(), row.clone());
        Ok(row)
    }

    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>> {
        let state = self.state.read().await;
        Ok(state.databases.get(name).cloned())
    }

    async fn update_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>> {
        let mut state = self.state.write().await;
        let entry = state
            .databases
            .get_mut(name)
            .ok_or_else(|| metastore_error::DatabaseNotFoundSnafu { db: name.clone() }.build())?;
        entry.update(database);
        Ok(entry.clone())
    }

    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> Result<()> {
        let mut state = self.state.write().await;
        let schema_keys: Vec<_> = state
            .schemas
            .keys()
            .filter(|(db, _)| db == name)
            .cloned()
            .collect();
        if !schema_keys.is_empty() && !cascade {
            return metastore_error::DatabaseInUseSnafu {
                database: name,
                schema: schema_keys
                    .iter()
                    .map(|(_, schema)| schema.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            }
            .fail();
        }
        for key in &schema_keys {
            state.schemas.remove(key);
        }
        let table_keys: Vec<_> = state
            .tables
            .keys()
            .filter(|(db, _, _)| db == name)
            .cloned()
            .collect();
        for key in &table_keys {
            state.tables.remove(key);
        }
        state.databases.remove(name);
        Ok(())
    }

    async fn list_schemas(&self, database: &DatabaseIdent) -> Result<Vec<RwObject<Schema>>> {
        let state = self.state.read().await;
        Ok(state
            .schemas
            .iter()
            .filter(|((db, _), _)| db == database)
            .map(|(_, schema)| schema.clone())
            .collect())
    }

    async fn create_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>> {
        let mut state = self.state.write().await;
        if state.schemas.contains_key(&Self::schema_key(ident)) {
            return metastore_error::SchemaAlreadyExistsSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if state.databases.get(&ident.database).is_none() {
            return metastore_error::DatabaseNotFoundSnafu {
                db: ident.database.clone(),
            }
            .fail();
        }
        let row = RwObject::new(schema);
        state.schemas.insert(Self::schema_key(ident), row.clone());
        Ok(row)
    }

    async fn get_schema(&self, ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>> {
        let state = self.state.read().await;
        Ok(state.schemas.get(&Self::schema_key(ident)).cloned())
    }

    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>> {
        let mut state = self.state.write().await;
        let entry = state
            .schemas
            .get_mut(&Self::schema_key(ident))
            .ok_or_else(|| {
                metastore_error::SchemaNotFoundSnafu {
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        entry.update(schema);
        Ok(entry.clone())
    }

    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> Result<()> {
        let mut state = self.state.write().await;
        let tables: Vec<_> = state
            .tables
            .keys()
            .filter(|(db, schema, _)| db == &ident.database && schema == &ident.schema)
            .cloned()
            .collect();
        if !tables.is_empty() && !cascade {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        for key in tables {
            state.tables.remove(&key);
        }
        state.schemas.remove(&Self::schema_key(ident));
        Ok(())
    }

    async fn list_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>> {
        let state = self.state.read().await;
        Ok(state
            .tables
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, table)| table.clone())
            .collect())
    }

    #[allow(clippy::too_many_lines)]
    async fn create_table(
        &self,
        ident: &TableIdent,
        mut table: TableCreateRequest,
    ) -> Result<RwObject<Table>> {
        let mut state = self.state.write().await;
        if state
            .schemas
            .get(&Self::schema_key(&ident.clone().into()))
            .is_none()
        {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }

        if state.tables.contains_key(&Self::table_key(ident)) {
            return metastore_error::TableAlreadyExistsSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }

        let schema_id = *table.schema.schema_id();
        let mut schemas = HashMap::new();
        schemas.insert(schema_id, table.schema.clone());

        let mut metadata_builder = TableMetadataBuilder::default();
        metadata_builder
            .schemas(schemas)
            .current_schema_id(schema_id)
            .location(table.location.clone().unwrap_or_default())
            .last_updated_ms(Utc::now().timestamp_millis())
            .format_version(FormatVersion::V2)
            .last_column_id(max_field_id(&table.schema));

        if let Some(spec) = table.partition_spec.take() {
            let spec_id = *spec.spec_id();
            let max_partition_id = spec
                .fields()
                .iter()
                .map(|field| *field.field_id())
                .max()
                .unwrap_or(0);
            let mut partition_specs = HashMap::new();
            partition_specs.insert(spec_id, spec);
            metadata_builder
                .partition_specs(partition_specs)
                .default_spec_id(spec_id)
                .last_partition_id(max_partition_id);
        }
        if let Some(order) = table.sort_order.take() {
            let order_id = order.order_id;
            let mut sort_orders = HashMap::new();
            sort_orders.insert(order_id, order);
            metadata_builder
                .sort_orders(sort_orders)
                .default_sort_order_id(order_id);
        }

        let mut metadata = metadata_builder
            .build()
            .context(metastore_error::TableMetadataBuilderSnafu)?;

        if metadata.properties.is_empty() {
            metadata.properties = HashMap::new();
        }

        let object_store = self
            .table_object_store_from_request(&state, &table, ident)
            .await?;
        let metadata_location = self
            .put_metadata(ident, object_store.clone(), &metadata)
            .await?;

        let mut properties = table.properties.take().unwrap_or_default();
        Self::update_properties_timestamps(&mut properties);

        let stored_table = Table {
            ident: ident.clone(),
            metadata,
            metadata_location,
            properties,
            volume_ident: table.volume_ident.clone(),
            volume_location: table.location,
            is_temporary: table.is_temporary.unwrap_or(false),
            format: table.format.unwrap_or(TableFormat::Iceberg),
        };

        let row = RwObject::new(stored_table);
        state.tables.insert(Self::table_key(ident), row.clone());
        Ok(row)
    }

    async fn get_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Table>>> {
        let state = self.state.read().await;
        Ok(state.tables.get(&Self::table_key(ident)).cloned())
    }

    async fn update_table(
        &self,
        ident: &TableIdent,
        update: TableUpdate,
    ) -> Result<RwObject<Table>> {
        let object_store = self.table_object_store(ident).await?.ok_or_else(|| {
            metastore_error::TableNotFoundSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .build()
        })?;

        let mut state = self.state.write().await;
        let table_entry = state
            .tables
            .get_mut(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::TableNotFoundSnafu {
                    table: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;

        for requirement in &update.requirements {
            TableRequirementExt::new(requirement.clone()).assert(&table_entry.metadata)?;
        }

        let mut metadata = table_entry.metadata.clone();
        apply_table_updates(&mut metadata, update.updates.clone())
            .context(metastore_error::IcebergSnafu)?;

        let metadata_location = self.put_metadata(ident, object_store, &metadata).await?;
        table_entry.data.metadata = metadata;
        table_entry.data.metadata_location = metadata_location;
        table_entry.touch();
        Ok(table_entry.clone())
    }

    async fn delete_table(&self, ident: &TableIdent, _cascade: bool) -> Result<()> {
        let mut state = self.state.write().await;
        state.tables.remove(&Self::table_key(ident));
        Ok(())
    }

    async fn table_object_store(&self, ident: &TableIdent) -> Result<Option<Arc<dyn ObjectStore>>> {
        let state = self.state.read().await;
        let volume_ident = state
            .tables
            .get(&Self::table_key(ident))
            .and_then(|table| table.volume_ident.clone());
        drop(state);
        if let Some(volume_ident) = volume_ident {
            self.volume_object_store(&volume_ident).await
        } else {
            Ok(None)
        }
    }

    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.tables.contains_key(&Self::table_key(ident)))
    }

    async fn url_for_table(&self, ident: &TableIdent) -> Result<String> {
        let state = self.state.read().await;
        if let Some(table) = state.tables.get(&Self::table_key(ident)) {
            Ok(table
                .volume_location
                .clone()
                .unwrap_or_else(|| format!("memory://{}", ident.table)))
        } else {
            metastore_error::TableNotFoundSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail()
        }
    }

    async fn volume_for_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Volume>>> {
        let state = self.state.read().await;
        let table = state.tables.get(&Self::table_key(ident));
        if let Some(table) = table {
            if let Some(volume_ident) = &table.volume_ident {
                return Ok(state.volumes.get(volume_ident).cloned());
            }
        }
        Ok(None)
    }
}

impl InMemoryMetastore {
    async fn table_object_store_from_request(
        &self,
        state: &MetastoreState,
        table: &TableCreateRequest,
        ident: &TableIdent,
    ) -> Result<Arc<dyn ObjectStore>> {
        let volume_ident = table.volume_ident.as_ref().ok_or_else(|| {
            metastore_error::TableVolumeMissingSnafu {
                table: ident.table.clone(),
            }
            .build()
        })?;
        Self::ensure_volume(state, volume_ident).await?;
        self.volume_object_store(volume_ident)
            .await?
            .ok_or_else(|| {
                metastore_error::VolumeNotFoundSnafu {
                    volume: volume_ident.clone(),
                }
                .build()
            })
    }

    fn update_properties_timestamps(properties: &mut HashMap<String, String>) {
        let utc_now = Utc::now();
        let utc_now_str = utc_now.to_rfc3339();
        properties.insert("created_at".to_string(), utc_now_str.clone());
        properties.insert("updated_at".to_string(), utc_now_str);
    }
}

fn max_field_id(schema: &IcebergSchema) -> i32 {
    fn recurse(field: &StructField) -> i32 {
        let mut max_id = field.id;
        if let Type::Struct(inner) = &field.field_type {
            for child in inner.iter() {
                max_id = max_id.max(recurse(child));
            }
        }
        max_id
    }

    schema.fields().iter().map(recurse).max().unwrap_or(0)
}
