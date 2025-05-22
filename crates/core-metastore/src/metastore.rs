use std::{collections::HashMap, sync::Arc};

use crate::error::{
    self as metastore_error, MetastoreError, MetastoreErrorKind, MetastoreResult,
    ObjectStoreSnafu, SerdeSnafu, TableMetadataBuilderSnafu, UrlParseSnafu, UtilSlateDBSnafu,
    IcebergSnafu, CreateDirectorySnafu, OperationFailedSnafu, ObjectStorePathSnafu,
};
use embucket_errors::wrap_error;

#[allow(clippy::wildcard_imports)]
use crate::models::*;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use core_utils::Db;
use core_utils::scan_iterator::{ScanIterator, VecScanIterator};
use dashmap::DashMap;
use futures::{StreamExt, TryStreamExt};
use iceberg_rust::catalog::commit::apply_table_updates;
use iceberg_rust_spec::table_metadata::{FormatVersion, TableMetadataBuilder};
use object_store::{ObjectStore, PutPayload, path::Path};
use serde::de::DeserializeOwned;
use snafu::ResultExt;
use uuid::Uuid;

#[async_trait]
pub trait Metastore: std::fmt::Debug + Send + Sync {
    fn iter_volumes(&self) -> MetastoreResult<VecScanIterator<RwObject<Volume>>>;
    async fn create_volume(&self, name: &VolumeIdent, volume: Volume) -> MetastoreResult<RwObject<Volume>>;
    async fn get_volume(&self, name: &VolumeIdent) -> MetastoreResult<Option<RwObject<Volume>>>;
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> MetastoreResult<RwObject<Volume>>;
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> MetastoreResult<()>;
    async fn volume_object_store(&self, name: &VolumeIdent) -> MetastoreResult<Arc<dyn ObjectStore>>;

    fn iter_databases(&self, volume_ident: Option<&VolumeIdent>) -> MetastoreResult<VecScanIterator<RwObject<Database>>>;
    async fn create_database(&self, name: &DatabaseIdent, database: Database) -> MetastoreResult<RwObject<Database>>;
    async fn get_database(&self, name: &DatabaseIdent) -> MetastoreResult<Option<RwObject<Database>>>;
    async fn update_database(&self, name: &DatabaseIdent, database: Database) -> MetastoreResult<RwObject<Database>>;
    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> MetastoreResult<()>;

    fn iter_schemas(&self, database: &DatabaseIdent) -> MetastoreResult<VecScanIterator<RwObject<Schema>>>;
    async fn create_schema(&self, ident: &SchemaIdent, schema: Schema) -> MetastoreResult<RwObject<Schema>>;
    async fn get_schema(&self, ident: &SchemaIdent) -> MetastoreResult<Option<RwObject<Schema>>>;
    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> MetastoreResult<RwObject<Schema>>;
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> MetastoreResult<()>;

    fn iter_tables(&self, schema: &SchemaIdent) -> MetastoreResult<VecScanIterator<RwObject<Table>>>;
    async fn create_table(&self, ident: &TableIdent, table: TableCreateRequest) -> MetastoreResult<RwObject<Table>>;
    async fn get_table(&self, ident: &TableIdent) -> MetastoreResult<Option<RwObject<Table>>>;
    async fn update_table(&self, ident: &TableIdent, update: TableUpdate) -> MetastoreResult<RwObject<Table>>;
    async fn delete_table(&self, ident: &TableIdent, cascade: bool) -> MetastoreResult<()>;
    async fn table_object_store(&self, ident: &TableIdent) -> MetastoreResult<Arc<dyn ObjectStore>>;

    async fn table_exists(&self, ident: &TableIdent) -> MetastoreResult<bool>;
    async fn url_for_table(&self, ident: &TableIdent) -> MetastoreResult<String>;
    async fn volume_for_table(&self, ident: &TableIdent) -> MetastoreResult<RwObject<Volume>>;
}

const KEY_VOLUME: &str = "vol";
const KEY_DATABASE: &str = "db";
const KEY_SCHEMA: &str = "sch";
const KEY_TABLE: &str = "tbl";

pub struct SlateDBMetastore {
    db: Db,
    object_store_cache: DashMap<VolumeIdent, Arc<dyn ObjectStore>>,
}

impl std::fmt::Debug for SlateDBMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBMetastore").finish()
    }
}

impl SlateDBMetastore {
    #[must_use]
    pub fn new(db: Db) -> Self { Self { db, object_store_cache: DashMap::new() } }
    pub async fn new_in_memory() -> Arc<Self> { Arc::new(Self::new(Db::memory().await)) }
    #[cfg(test)]
    #[must_use]
    pub const fn db(&self) -> &Db { &self.db }

    fn iter_objects<T>(&self, iter_key: String) -> MetastoreResult<VecScanIterator<RwObject<T>>>
    where T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync {
        self.db.iter_objects(iter_key)
            .map_err(|e| wrap_error(e, format!("Failed to iterate objects with key prefix '{}'", iter_key)))
            .context(UtilSlateDBSnafu)
    }

    async fn create_object<T>(&self, key: &str, object_type: &str, object: T) -> MetastoreResult<RwObject<T>>
    where T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync {
        let get_res = self.db.get::<RwObject<T>>(key).await
            .map_err(|e| wrap_error(e, format!("DB get failed for key '{}' during create_object", key)))
            .context(UtilSlateDBSnafu)?;
        if get_res.is_none() {
            let rwobject = RwObject::new(object);
            self.db.put(key, &rwobject).await
                .map_err(|e| wrap_error(e, format!("DB put failed for key '{}' during create_object", key)))
                .context(UtilSlateDBSnafu)?;
            Ok(rwobject)
        } else {
            let kind = MetastoreErrorKind::ObjectAlreadyExists { type_name: object_type.to_string(), name: key.to_string() };
            OperationFailedSnafu { source: wrap_error(kind, format!("Cannot create {}: object already exists at key '{}'", object_type, key)) }.fail()
        }
    }

    async fn update_object<T>(&self, key: &str, object: T) -> MetastoreResult<RwObject<T>>
    where T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync {
        let get_res = self.db.get::<RwObject<T>>(key).await
            .map_err(|e| wrap_error(e, format!("DB get failed for key '{}' during update_object", key)))
            .context(UtilSlateDBSnafu)?;
        if let Some(mut rwo) = get_res {
            rwo.update(object);
            self.db.put(key, &rwo).await
                .map_err(|e| wrap_error(e, format!("DB put failed for key '{}' during update_object", key)))
                .context(UtilSlateDBSnafu)?;
            Ok(rwo)
        } else {
            let kind = MetastoreErrorKind::ObjectNotFound;
            OperationFailedSnafu { source: wrap_error(kind, format!("Object not found at key '{}' for update", key)) }.fail()
        }
    }

    async fn delete_object(&self, key: &str) -> MetastoreResult<()> {
        self.db.delete(key).await
            .map_err(|e| wrap_error(e, format!("DB delete failed for key '{}'", key)))
            .context(UtilSlateDBSnafu)
    }
    fn generate_metadata_filename() -> String { format!("{}.metadata.json", Uuid::new_v4()) }
    pub fn update_properties_timestamps(properties: &mut HashMap<String, String>) {
        let utc_now_str = Utc::now().to_rfc3339();
        properties.insert("created_at".to_string(), utc_now_str.clone());
        properties.insert("updated_at".to_string(), utc_now_str);
    }
    #[must_use]
    pub fn get_default_properties() -> HashMap<String, String> {
        let mut properties = HashMap::new(); Self::update_properties_timestamps(&mut properties); properties
    }
}

#[async_trait]
impl Metastore for SlateDBMetastore {
    fn iter_volumes(&self) -> MetastoreResult<VecScanIterator<RwObject<Volume>>> {
        self.iter_objects(KEY_VOLUME.to_string())
    }
    async fn create_volume(&self, name: &VolumeIdent, volume: Volume) -> MetastoreResult<RwObject<Volume>> {
        let key = format!("{KEY_VOLUME}/{name}");
        let object_store = volume.get_object_store().map_err(|e_meta| 
            wrap_error(e_meta, format!("Invalid volume configuration for '{}'", name))
        ).context(OperationFailedSnafu)?;
        let create_res = self.create_object(&key, "volume", volume).await;
        match create_res {
            Ok(rwobject) => { self.object_store_cache.insert(name.clone(), object_store); Ok(rwobject) }
            Err(e) => {
                if let MetastoreError::OperationFailed { source: emb_err } = &e {
                    if let MetastoreErrorKind::ObjectAlreadyExists { .. } = emb_err.source {
                        let kind = MetastoreErrorKind::VolumeAlreadyExists { volume: name.clone() };
                        return OperationFailedSnafu { source: wrap_error(kind, format!("Volume '{}' already exists.", name)) }.fail();
                    }
                }
                Err(wrap_error(e, format!("Failed to store new volume definition for '{}'", name))).context(OperationFailedSnafu)
            }
        }
    }
    async fn get_volume(&self, name: &VolumeIdent) -> MetastoreResult<Option<RwObject<Volume>>> {
        let key = format!("{KEY_VOLUME}/{name}");
        self.db.get(&key).await.map_err(|e| wrap_error(e, format!("Database lookup for volume '{}' failed", name))).context(UtilSlateDBSnafu)
    }
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> MetastoreResult<RwObject<Volume>> {
        let object_store = volume.get_object_store().map_err(|e_meta| wrap_error(e_meta, format!("Invalid updated volume config for '{}'", name))).context(OperationFailedSnafu)?;
        self.get_volume(name).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::VolumeNotFound { volume: name.clone() };
            OperationFailedSnafu { source: wrap_error(kind, format!("Volume '{}' not found for update.", name)) }.build()
        })?;
        let key = format!("{KEY_VOLUME}/{name}");
        let updated_rwo = self.update_object(&key, volume).await.map_err(|e_meta| wrap_error(e_meta, format!("Failed to store updated volume definition for '{}'", name))).context(OperationFailedSnafu)?;
        self.object_store_cache.alter(name, |_, _| object_store);
        Ok(updated_rwo)
    }
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> MetastoreResult<()> {
        self.get_volume(name).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::VolumeNotFound { volume: name.clone() };
            OperationFailedSnafu { source: wrap_error(kind, format!("Cannot delete volume '{}': not found.", name)) }.build()
        })?;
        let key = format!("{KEY_VOLUME}/{name}");
        let dbs_using_vol = self.iter_databases(Some(name))?.collect::<MetastoreResult<Vec<_>>>().await?;
        if !dbs_using_vol.is_empty() && !cascade {
            let db_names = dbs_using_vol.iter().map(|db| db.data.ident.as_str()).collect::<Vec<_>>().join(", ");
            let kind = MetastoreErrorKind::VolumeInUse { database: db_names };
            return OperationFailedSnafu { source: wrap_error(kind, format!("Volume '{}' is in use and cascade is false.", name)) }.fail();
        }
        for db_rwo in dbs_using_vol {
            self.delete_database(&db_rwo.data.ident, cascade).await.map_err(|e_meta| wrap_error(e_meta, format!("Failed to cascade delete database '{}' for volume '{}'", db_rwo.data.ident, name))).context(OperationFailedSnafu)?;
        }
        self.delete_object(&key).await?;
        self.object_store_cache.remove(name);
        Ok(())
    }
    async fn volume_object_store(&self, name: &VolumeIdent) -> MetastoreResult<Arc<dyn ObjectStore>> {
        if let Some(store_entry) = self.object_store_cache.get(name) { return Ok(store_entry.value().clone()); }
        let volume_rwo = self.get_volume(name).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::VolumeNotFound { volume: name.clone() };
            OperationFailedSnafu { source: wrap_error(kind, format!("Volume '{}' not found when trying to get its object store", name)) }.build()
        })?;
        let object_store = volume_rwo.data.get_object_store().map_err(|e_meta| wrap_error(e_meta, format!("Failed to initialize object store for volume '{}'", name))).context(OperationFailedSnafu)?;
        self.object_store_cache.insert(name.clone(), object_store.clone());
        Ok(object_store)
    }
    fn iter_databases(&self, volume_ident: Option<&VolumeIdent>) -> MetastoreResult<VecScanIterator<RwObject<Database>>> {
        let iter = self.iter_objects(KEY_DATABASE.to_string())?;
        if let Some(vol_id) = volume_ident {
            let vol_id_owned = vol_id.to_owned();
            // This filtering is inefficient for large numbers of databases.
            // A real implementation might use secondary indexes or specific scan prefixes if the DB supports it.
            // For now, it collects all, then filters. This must be async and handle errors.
            let collected_dbs = iter.collect::<MetastoreResult<Vec<_>>>().map(|dbs| {
                dbs.into_iter().filter(|db_rwo| db_rwo.data.volume == vol_id_owned).collect::<Vec<_>>()
            })?; // This unwrap_or_default was problematic. Now propagates error.
            Ok(VecScanIterator::new(collected_dbs))
        } else { Ok(iter) }
    }
    async fn create_database(&self, name: &DatabaseIdent, database: Database) -> MetastoreResult<RwObject<Database>> {
        self.volume_object_store(&database.volume).await.map_err(|e_meta| wrap_error(e_meta, format!("Invalid volume '{}' for database '{}'", database.volume, name))).context(OperationFailedSnafu)?;
        let key = format!("{KEY_DATABASE}/{name}");
        self.create_object(&key, "database", database).await.map_err(|e_meta|
            match &e_meta {
                MetastoreError::OperationFailed{ source: emb } if matches!(emb.source, MetastoreErrorKind::ObjectAlreadyExists{..}) => {
                    let kind = MetastoreErrorKind::DatabaseAlreadyExists { db: name.clone() };
                    OperationFailedSnafu{ source: wrap_error(kind, emb.context.clone()) }.build()
                }
                _ => wrap_error(e_meta, format!("Failed to store database definition for '{}'", name)).context(OperationFailedSnafu).unwrap_err()
            }
        )
    }
    async fn get_database(&self, name: &DatabaseIdent) -> MetastoreResult<Option<RwObject<Database>>> {
        let key = format!("{KEY_DATABASE}/{name}");
        self.db.get(&key).await.map_err(|e| wrap_error(e, format!("DB lookup for database '{}'", name))).context(UtilSlateDBSnafu)
    }
    async fn update_database(&self, name: &DatabaseIdent, database: Database) -> MetastoreResult<RwObject<Database>> {
        self.get_database(name).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::DatabaseNotFound { db: name.clone() };
            OperationFailedSnafu{ source: wrap_error(kind, format!("Database '{}' not found for update", name)) }.build()
        })?;
        if database.ident != *name {
            let kind = MetastoreErrorKind::Validation{ source: { let mut ve = validator::ValidationErrors::new(); ve.add_field_error("ident", validator::ValidationError::new("identifier_mismatch")); ve } };
            return OperationFailedSnafu{ source: wrap_error(kind, format!("Database ident mismatch: path is '{}', payload ident is '{}'", name, database.ident)) }.fail();
        }
        self.volume_object_store(&database.volume).await.map_err(|e| wrap_error(e, format!("Invalid target volume '{}' for database update of '{}'", database.volume, name))).context(OperationFailedSnafu)?;
        let key = format!("{KEY_DATABASE}/{name}");
        self.update_object(&key, database).await.map_err(|e| wrap_error(e, format!("Failed to store updated database definition for '{}'", name))).context(OperationFailedSnafu)
    }
     async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> MetastoreResult<()> {
        self.get_database(name).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::DatabaseNotFound { db: name.clone() };
            OperationFailedSnafu{source: wrap_error(kind, format!("Cannot delete database '{}': not found", name))}.build()
        })?;
        let schemas = self.iter_schemas(name)?.collect::<MetastoreResult<Vec<_>>>().await?;
        if !schemas.is_empty() && !cascade {
            let schema_names = schemas.iter().map(|s| s.data.ident.schema.as_str()).collect::<Vec<_>>().join(", ");
            let kind = MetastoreErrorKind::Validation{ source: { let mut ve = validator::ValidationErrors::new(); ve.add_field_error("cascade", validator::ValidationError::new("dependent_objects_exist")); ve } };
            return OperationFailedSnafu{source: wrap_error(kind, format!("Database '{}' contains schemas ({}) and cascade is false", name, schema_names))}.fail();
        }
        for schema_rwo in schemas {
            self.delete_schema(&schema_rwo.data.ident, cascade).await.map_err(|e| wrap_error(e, format!("Cascade delete of schema '{}' failed for database '{}'", schema_rwo.data.ident.schema, name))).context(OperationFailedSnafu)?;
        }
        let key = format!("{KEY_DATABASE}/{name}");
        self.delete_object(&key).await
    }
    fn iter_schemas(&self, database_ident: &DatabaseIdent) -> MetastoreResult<VecScanIterator<RwObject<Schema>>> {
        let key = if database_ident.is_empty() { KEY_SCHEMA.to_string() } else { format!("{KEY_SCHEMA}/{}", database_ident) };
        self.iter_objects(key)
    }
    async fn create_schema(&self, ident: &SchemaIdent, schema: Schema) -> MetastoreResult<RwObject<Schema>> {
        self.get_database(&ident.database).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::DatabaseNotFound { db: ident.database.clone() };
            OperationFailedSnafu{source: wrap_error(kind, format!("Database '{}' not found for schema '{}'", ident.database, ident.schema))}.build()
        })?;
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.create_object(&key, "schema", schema).await.map_err(|e_meta|
            match &e_meta {
                MetastoreError::OperationFailed{source: emb} if matches!(emb.source, MetastoreErrorKind::ObjectAlreadyExists{..}) => {
                    let kind = MetastoreErrorKind::SchemaAlreadyExists{schema: ident.schema.clone(), db: ident.database.clone()};
                    OperationFailedSnafu{source: wrap_error(kind, emb.context.clone())}.build()
                }
                _ => wrap_error(e_meta, format!("Failed to store schema definition '{}/{}'", ident.database, ident.schema)).context(OperationFailedSnafu).unwrap_err()
            }
        )
    }
    async fn get_schema(&self, ident: &SchemaIdent) -> MetastoreResult<Option<RwObject<Schema>>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.db.get(&key).await.map_err(|e| wrap_error(e, format!("DB lookup for schema '{}/{}'", ident.database, ident.schema))).context(UtilSlateDBSnafu)
    }
    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> MetastoreResult<RwObject<Schema>> {
        self.get_schema(ident).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::SchemaNotFound{schema: ident.schema.clone(), db: ident.database.clone()};
            OperationFailedSnafu{source: wrap_error(kind, format!("Schema '{}/{}' not found for update", ident.database, ident.schema))}.build()
        })?;
        if schema.ident != *ident {
            let kind = MetastoreErrorKind::Validation{source: { let mut ve = validator::ValidationErrors::new(); ve.add_field_error("ident", validator::ValidationError::new("identifier_mismatch")); ve }};
            return OperationFailedSnafu{source: wrap_error(kind, format!("Schema ident mismatch: path is '{}/{}', payload is '{}/{}'", ident.database, ident.schema, schema.ident.database, schema.ident.schema))}.fail();
        }
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.update_object(&key, schema).await.map_err(|e| wrap_error(e, format!("Failed to store updated schema '{}/{}'", ident.database, ident.schema))).context(OperationFailedSnafu)
    }
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> MetastoreResult<()> {
        self.get_schema(ident).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::SchemaNotFound{schema: ident.schema.clone(), db: ident.database.clone()};
            OperationFailedSnafu{source: wrap_error(kind, format!("Cannot delete schema '{}/{}': not found", ident.database, ident.schema))}.build()
        })?;
        let tables = self.iter_tables(ident)?.collect::<MetastoreResult<Vec<_>>>().await?;
        if !tables.is_empty() && !cascade {
            let table_names = tables.iter().map(|t| t.data.ident.table.as_str()).collect::<Vec<_>>().join(", ");
            let kind = MetastoreErrorKind::Validation{source: { let mut ve = validator::ValidationErrors::new(); ve.add_field_error("cascade", validator::ValidationError::new("dependent_objects_exist")); ve }};
            return OperationFailedSnafu{source: wrap_error(kind, format!("Schema '{}/{}' contains tables ({}) and cascade is false", ident.database, ident.schema, table_names))}.fail();
        }
        for table_rwo in tables {
            self.delete_table(&table_rwo.data.ident, cascade).await.map_err(|e| wrap_error(e, format!("Cascade delete of table '{}' failed for schema '{}/{}'", table_rwo.data.ident.table, ident.database, ident.schema))).context(OperationFailedSnafu)?;
        }
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.delete_object(&key).await
    }
    fn iter_tables(&self, schema_ident: &SchemaIdent) -> MetastoreResult<VecScanIterator<RwObject<Table>>> {
        let key = if schema_ident.schema.is_empty() && schema_ident.database.is_empty() { KEY_TABLE.to_string() } else { format!("{KEY_TABLE}/{}/{}", schema_ident.database, schema_ident.schema) };
        self.iter_objects(key)
    }
    #[allow(clippy::too_many_lines)]
    async fn create_table(&self, ident: &TableIdent, mut table_req: TableCreateRequest) -> MetastoreResult<RwObject<Table>> {
        self.get_schema(&ident.clone().into()).await?.ok_or_else(|| {
             let kind = MetastoreErrorKind::SchemaNotFound { schema: ident.schema.clone(), db: ident.database.clone() };
             OperationFailedSnafu { source: wrap_error(kind, format!("Schema '{}/{}' not found for table '{}'", ident.database, ident.schema, ident.table)) }.build()
        })?;
        let key = format!("{KEY_TABLE}/{}/{}/{}", ident.database, ident.schema, ident.table);

        let table_location = if table_req.is_temporary.unwrap_or_default() {
            let volume_ident_str: String = table_req.volume_ident.as_ref().map_or_else(|| Uuid::new_v4().to_string(), ToString::to_string);
            let volume_payload = Volume { ident: volume_ident_str.clone(), volume: VolumeType::Memory };
            // Ensure create_volume is called with a owned String for name, not a reference.
            let created_volume = self.create_volume(&volume_ident_str.clone(), volume_payload).await.map_err(|e| wrap_error(e, "Failed to create temporary volume for table".to_string())).context(OperationFailedSnafu)?;
            if table_req.volume_ident.is_none() { table_req.volume_ident = Some(volume_ident_str); } // Assign back the potentially generated ID
            table_req.location.as_ref().map_or_else(|| created_volume.data.prefix(), |loc| format!("{}/{}", created_volume.data.prefix().trim_end_matches('/'), loc.trim_start_matches('/')))
        } else {
            // For non-temporary tables, if location is not provided, it defaults based on volume structure.
            // We need the volume to construct this default path.
            let vol_rwo = self.volume_for_table(ident).await?; // This gets the correct volume (table's own or DB's)
            let vol_prefix = vol_rwo.data.prefix();
            format!("{}/{}/{}/{}", vol_prefix.trim_end_matches('/'), ident.database, ident.schema, ident.table)
        };
        let final_table_location = table_req.location.as_ref().unwrap_or(&table_location);

        let metadata_filename = Self::generate_metadata_filename();
        let metadata_location = format!("{}/metadata/{}", final_table_location.trim_end_matches('/'), metadata_filename);
        
        let mut tb_builder = TableMetadataBuilder::default();
        tb_builder.current_schema_id(*table_req.schema.schema_id()).with_schema((0, table_req.schema.clone())).format_version(FormatVersion::V2) // Clone schema for builder
                  .location(final_table_location.clone());
        if let Some(props) = table_req.properties.as_ref() { tb_builder.properties(props.clone()); }
        if let Some(spec) = table_req.partition_spec.clone() { tb_builder.with_partition_spec((0, spec)); } // Clone spec
        if let Some(order) = table_req.sort_order.clone() { tb_builder.with_sort_order((0, order)); } // Clone order
        let table_metadata = tb_builder.build().map_err(|e| wrap_error(e, "Building table metadata".to_string())).context(TableMetadataBuilderSnafu)?;
        
        let mut final_props = table_req.properties.unwrap_or_default(); // Take ownership
        Self::update_properties_timestamps(&mut final_props);

        let table_to_create = Table {
            ident: ident.clone(), metadata: table_metadata.clone(), metadata_location, properties: final_props,
            volume_ident: table_req.volume_ident, volume_location: table_req.location, // Store original requested location
            is_temporary: table_req.is_temporary.unwrap_or_default(), format: table_req.format.unwrap_or(TableFormat::Iceberg),
        };
        let rwo_table = self.create_object(&key, "table", table_to_create.clone()).await.map_err(|e_meta|
            match &e_meta {
                MetastoreError::OperationFailed{source: emb} if matches!(emb.source, MetastoreErrorKind::ObjectAlreadyExists{..}) => {
                    let kind = MetastoreErrorKind::TableAlreadyExists{table: ident.table.clone(), schema: ident.schema.clone(), db: ident.database.clone()};
                    OperationFailedSnafu{source: wrap_error(kind, emb.context.clone())}.build()
                }
                _ => wrap_error(e_meta, format!("Storing table definition '{}/{}/{}'", ident.database, ident.schema, ident.table)).context(OperationFailedSnafu).unwrap_err()
            }
        )?;

        let obj_store = self.table_object_store(ident).await?;
        let data_bytes = Bytes::from(serde_json::to_vec(&table_metadata).map_err(|e| wrap_error(e, "Serializing table metadata".to_string())).context(SerdeSnafu)?);
        let md_path_url = url::Url::parse(&rwo_table.data.metadata_location).map_err(|e| wrap_error(e, "Parsing metadata URL for put".to_string())).context(UrlParseSnafu)?;
        let obj_store_path = Path::from(md_path_url.path());
        obj_store.put(&obj_store_path, PutPayload::from(data_bytes)).await.map_err(|e| wrap_error(e, "Putting table metadata to object store".to_string())).context(ObjectStoreSnafu)?;
        Ok(rwo_table)
    }
    async fn get_table(&self, ident: &TableIdent) -> MetastoreResult<Option<RwObject<Table>>> {
        let key = format!("{KEY_TABLE}/{}/{}/{}", ident.database, ident.schema, ident.table);
        self.db.get(&key).await.map_err(|e| wrap_error(e, format!("DB lookup for table '{}/{}/{}'", ident.database, ident.schema, ident.table))).context(UtilSlateDBSnafu)
    }
    async fn update_table(&self, ident: &TableIdent, update: TableUpdate) -> MetastoreResult<RwObject<Table>> {
        let mut table = self.get_table(ident).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::TableNotFound { table: ident.table.clone(), schema: ident.schema.clone(), db: ident.database.clone() };
            OperationFailedSnafu{source: wrap_error(kind, format!("Table '{}/{}/{}' not found for update", ident.database, ident.schema, ident.table))}.build()
        })?.data;

        update.requirements.into_iter().map(TableRequirementExt::new).try_for_each(|req| req.assert(&table.metadata, true).map_err(|e_str| {
            let kind = MetastoreErrorKind::TableRequirementFailed { message: e_str };
            OperationFailedSnafu{source: wrap_error(kind, "Table update requirement failed".to_string())}.build()
        }))?;

        apply_table_updates(&mut table.metadata, update.updates).map_err(|e| wrap_error(e, "Applying Iceberg table updates".to_string())).context(IcebergSnafu)?;
        Self::update_properties_timestamps(&mut table.properties);
        
        let table_base_storage_location = self.url_for_table(ident).await?;
        table.metadata_location = format!("{}/metadata/{}", table_base_storage_location.trim_end_matches('/'), Self::generate_metadata_filename());

        let key = format!("{KEY_TABLE}/{}/{}/{}", ident.database, ident.schema, ident.table);
        let rw_table = self.update_object(&key, table.clone()).await.map_err(|e| wrap_error(e, "Storing updated table definition".to_string())).context(OperationFailedSnafu)?;

        let obj_store = self.table_object_store(ident).await?;
        let data_bytes = Bytes::from(serde_json::to_vec(&table.metadata).map_err(|e| wrap_error(e, "Serializing updated metadata".to_string())).context(SerdeSnafu)?);
        let md_path_url = url::Url::parse(&table.metadata_location).map_err(|e| wrap_error(e, "Parsing updated metadata URL for put".to_string())).context(UrlParseSnafu)?;
        let obj_store_path = Path::from(md_path_url.path());
        obj_store.put(&obj_store_path, PutPayload::from(data_bytes)).await.map_err(|e| wrap_error(e, "Putting updated metadata to object store".to_string())).context(ObjectStoreSnafu)?;
        Ok(rw_table)
    }
    async fn delete_table(&self, ident: &TableIdent, cascade: bool) -> MetastoreResult<()> {
        let table_rwo = self.get_table(ident).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::TableNotFound{table: ident.table.clone(), schema: ident.schema.clone(), db: ident.database.clone()};
            OperationFailedSnafu{source: wrap_error(kind, format!("Cannot delete table '{}/{}/{}': not found", ident.database, ident.schema, ident.table))}.build()
        })?;
        if cascade {
            let obj_store = self.table_object_store(ident).await?;
            let table_data_dir_str = table_rwo.data.metadata.location();
            let base_url = url::Url::parse(table_data_dir_str).map_err(|e| wrap_error(e, format!("Parsing table base data URL for deletion: {}", table_data_dir_str))).context(UrlParseSnafu)?;
            let data_dir_path = Path::from(base_url.path());

            if !data_dir_path.as_ref().is_empty() && data_dir_path.as_ref() != "/" {
                // List and delete objects under the table's directory. This is a broad approach.
                // A more precise approach might involve deleting specific metadata files and data files based on manifests.
                // For simplicity, this example attempts to delete everything under the table's location.
                // This might need adjustment based on how data files are stored relative to metadata.location.
                // If metadata.location points to a specific metadata file, its parent is the actual table root.
                let path_to_list = if table_data_dir_str.ends_with(".metadata.json") {
                    data_dir_path.parent().unwrap_or(&data_dir_path) // List parent dir if location is a metadata file
                } else {
                    &data_dir_path // Assume location is a directory
                };

                let locations_to_delete = obj_store.list(Some(path_to_list)).map_ok(|m| m.location).boxed();
                obj_store.delete_stream(locations_to_delete).try_collect::<Vec<Path>>().await.map_err(|e| wrap_error(e, format!("Deleting table data/metadata from object store at path {}", path_to_list))).context(ObjectStoreSnafu)?;
            } else {
                let kind = MetastoreErrorKind::Validation { source: { let mut ve = validator::ValidationErrors::new(); ve.add_field_error("path", validator::ValidationError::new("invalid_delete_path")); ve } };
                return OperationFailedSnafu{source: wrap_error(kind, format!("Refusing to delete data for table '{}/{}/{}' due to invalid or root path: {}", ident.database, ident.schema, ident.table, data_dir_path))}.fail();
            }
        }
        if table_rwo.data.is_temporary {
            if let Some(vol_ident) = &table_rwo.data.volume_ident {
                self.delete_volume(vol_ident, false).await.map_err(|e| wrap_error(e, format!("Deleting temporary volume '{}' for table", vol_ident))).context(OperationFailedSnafu)?;
            } else {
                let kind = MetastoreErrorKind::VolumeNotFound{ volume: "unknown_for_temporary_table".to_string() };
                 return OperationFailedSnafu{source: wrap_error(kind, format!("Temporary table '{}/{}/{}' is missing its volume identifier for cleanup.", ident.database, ident.schema, ident.table))}.fail();
            }
        }
        let key = format!("{KEY_TABLE}/{}/{}/{}", ident.database, ident.schema, ident.table);
        self.delete_object(&key).await
    }
    async fn table_object_store(&self, ident: &TableIdent) -> MetastoreResult<Arc<dyn ObjectStore>> {
        let vol_rwo = self.volume_for_table(ident).await?;
        self.volume_object_store(&vol_rwo.data.ident).await
    }
    async fn table_exists(&self, ident: &TableIdent) -> MetastoreResult<bool> { self.get_table(ident).await.map(|t| t.is_some()) }
    async fn url_for_table(&self, ident: &TableIdent) -> MetastoreResult<String> {
        let tbl_rwo = self.get_table(ident).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::TableNotFound { table: ident.table.clone(), schema: ident.schema.clone(), db: ident.database.clone() };
            OperationFailedSnafu{source: wrap_error(kind, format!("Table '{}/{}/{}' not found when building URL", ident.database, ident.schema, ident.table))}.build()
        })?;
        let tbl_data = tbl_rwo.data;
        if !tbl_data.metadata.location.is_empty() { return Ok(tbl_data.metadata.location.clone()); }
        let vol_rwo = self.volume_for_table(ident).await?;
        let vol_prefix = vol_rwo.data.prefix();
        if let Some(table_specific_loc) = tbl_data.volume_location.as_ref() {
             return Ok(format!("{}/{}", vol_prefix.trim_end_matches('/'), table_specific_loc.trim_start_matches('/')));
        }
        Ok(format!("{}/{}/{}/{}", vol_prefix.trim_end_matches('/'), ident.database, ident.schema, ident.table))
    }
    async fn volume_for_table(&self, ident: &TableIdent) -> MetastoreResult<RwObject<Volume>> {
        let table_rwo = self.get_table(ident).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::TableNotFound{table: ident.table.clone(), schema: ident.schema.clone(), db: ident.database.clone()};
            OperationFailedSnafu{source: wrap_error(kind, format!("Table '{}/{}/{}' not found when resolving volume", ident.database, ident.schema, ident.table))}.build()
        })?;
        let vol_ident_to_get = if let Some(vol_id_on_table) = &table_rwo.data.volume_ident {
            vol_id_on_table.clone()
        } else {
            let db_rwo = self.get_database(&ident.database).await?.ok_or_else(|| {
                let kind = MetastoreErrorKind::DatabaseNotFound { db: ident.database.clone() };
                OperationFailedSnafu{source: wrap_error(kind, format!("Database '{}' for table '{}/{}/{}' not found", ident.database, ident.database, ident.schema, ident.table))}.build()
            })?;
            db_rwo.data.volume.clone()
        };
        self.get_volume(&vol_ident_to_get).await?.ok_or_else(|| {
            let kind = MetastoreErrorKind::VolumeNotFound { volume: vol_ident_to_get.clone() };
            OperationFailedSnafu{source: wrap_error(kind, format!("Volume '{}' for table '{}/{}/{}' not found", vol_ident_to_get, ident.database, ident.schema, ident.table))}.build()
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use iceberg_rust_spec::{schema::Schema as IcebergSchema, types::{PrimitiveType, StructField, Type}};
    use slatedb::Db as SlateDbInternal;
    use std::sync::Arc;
    use crate::error::MetastoreErrorKind;

    fn insta_filters() -> Vec<(&'static str, &'static str)> {
        vec![
            (r"created_at[^,]*", "created_at: \"TIMESTAMP\""),
            (r"updated_at[^,]*", "updated_at: \"TIMESTAMP\""),
            (r"last_modified[^,]*", "last_modified: \"TIMESTAMP\""),
            (r"size[^,]*", "size: \"INTEGER\""),
            (r"last_updated_ms[^,]*", "last_update_ms: \"INTEGER\""),
            (r"backtrace: BacktraceAt \{ location: \".*\" \}", "backtrace: BacktraceAt { location: \"REDACTED\" }"),
            (r"backtrace: Backtrace \{ state: \d+ \}", "backtrace: Backtrace { ... }"),
            (r"backtrace: Backtrace at .*", "backtrace: Backtrace { ... }"),
            (r"([0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12})", "UUID"),
            (r"lookup: \{[^}]*\}", "lookup: {LOOKUPS}"),
            (r"properties: \{[^}]*\}", "properties: {PROPERTIES}"),
        ]
    }

    async fn get_metastore() -> SlateDBMetastore {
        let object_store = object_store::memory::InMemory::new();
        let sdb = SlateDbInternal::open(Path::from("/"), Arc::new(object_store)).await.expect("TEST SETUP: Failed to open db");
        let db_wrapper = Db::new(Arc::new(sdb));
        SlateDBMetastore::new(db_wrapper)
    }

    #[tokio::test]
    async fn test_create_volumes_success() {
        let ms = get_metastore().await;
        let volume_name = "test_vol_create_ok".to_string();
        let volume = Volume::new(volume_name.clone(), VolumeType::Memory);
        let creation_result = ms.create_volume(&volume_name, volume).await;
        assert!(creation_result.is_ok(), "create_volume failed: {:?}", creation_result.err());
    }

    #[tokio::test]
    async fn test_duplicate_volume_error() {
        let ms = get_metastore().await;
        let volume_name = "test_dup_vol_err".to_string();
        let volume1 = Volume::new(volume_name.clone(), VolumeType::Memory);
        assert!(ms.create_volume(&volume_name, volume1).await.is_ok());
        let volume2 = Volume::new(volume_name.clone(), VolumeType::Memory);
        let duplicate_result = ms.create_volume(&volume_name, volume2).await;
        assert!(duplicate_result.is_err());
        let err = duplicate_result.unwrap_err();
        match err {
            MetastoreError::OperationFailed { source: emb_err } => {
                match emb_err.source {
                    MetastoreErrorKind::VolumeAlreadyExists { volume } => assert_eq!(volume, volume_name),
                    _ => panic!("Incorrect MetastoreErrorKind: {:?}", emb_err.source),
                }
                assert!(emb_err.context.contains("already exists"));
            }
            _ => panic!("Incorrect MetastoreError variant: {:?}", err),
        }
        insta::with_settings!({ filters => insta_filters(), }, {
            insta::assert_debug_snapshot!("duplicate_volume_error_snapshot", err);
        });
    }
    
    #[tokio::test]
    async fn test_delete_volume_success_and_not_found() {
        let ms = get_metastore().await;
        let volume_name = "vol_to_delete_ok".to_string();
        let volume = Volume::new(volume_name.clone(), VolumeType::Memory);
        assert!(ms.create_volume(&volume_name, volume).await.is_ok());
        assert!(ms.delete_volume(&volume_name, false).await.is_ok());
        let get_res = ms.get_volume(&volume_name).await;
        assert!(get_res.is_ok() && get_res.unwrap().is_none(), "Volume should be deleted");
        let del_non_existent_res = ms.delete_volume(&"does_not_exist_again".to_string(), false).await;
        assert!(del_non_existent_res.is_err());
        match del_non_existent_res.unwrap_err() {
            MetastoreError::OperationFailed { source: emb_err } => {
                matches!(emb_err.source, MetastoreErrorKind::VolumeNotFound { .. });
            }
            _ => panic!("Expected VolumeNotFound error"),
        }
    }
    
    #[tokio::test]
    async fn test_update_volume_success_and_not_found() {
        let ms = get_metastore().await;
        let volume_name = "vol_to_update_ok".to_string();
        let initial_volume = Volume::new(volume_name.clone(), VolumeType::Memory);
        let created_rwo = ms.create_volume(&volume_name, initial_volume).await.unwrap();
        let updated_volume_payload = Volume::new(volume_name.clone(), VolumeType::File(FileVolume { path: "/tmp/newpath".to_string() }));
        let update_res = ms.update_volume(&volume_name, updated_volume_payload.clone()).await;
        assert!(update_res.is_ok(), "Update volume failed: {:?}", update_res.err());
        let updated_rwo_from_res = update_res.unwrap();
        assert_ne!(created_rwo.data.volume, updated_rwo_from_res.data.volume);
        let non_existent_name = "vol_does_not_exist_for_update".to_string();
        let update_non_existent_res = ms.update_volume(&non_existent_name, updated_volume_payload).await;
        assert!(update_non_existent_res.is_err());
        match update_non_existent_res.unwrap_err() {
            MetastoreError::OperationFailed { source: emb_err } => {
                matches!(emb_err.source, MetastoreErrorKind::VolumeNotFound { .. });
            }
            _ => panic!("Expected VolumeNotFound error for update"),
        }
    }

    #[tokio::test]
    async fn test_delete_volume_in_use() {
        let ms = get_metastore().await;
        let volume_name = "vol_in_use_test_del".to_string(); // Unique name
        let db_name = "db_on_vol_in_use_test_del".to_string();
        let volume = Volume::new(volume_name.clone(), VolumeType::Memory);
        assert!(ms.create_volume(&volume_name, volume).await.is_ok());
        let database = Database { ident: db_name.clone(), volume: volume_name.clone(), properties: None };
        assert!(ms.create_database(&db_name, database).await.is_ok());
        let delete_res = ms.delete_volume(&volume_name, false).await;
        assert!(delete_res.is_err());
        match delete_res.unwrap_err() {
            MetastoreError::OperationFailed { source: emb_err } => {
                match emb_err.source {
                    MetastoreErrorKind::VolumeInUse { database } => assert!(database.contains(&db_name)), // Check if db_name is part of the string
                    _ => panic!("Incorrect MetastoreErrorKind: {:?}", emb_err.source),
                }
            }
            other_err => panic!("Expected VolumeInUse error, got {:?}", other_err),
        }
    }

    #[tokio::test]
    async fn test_schemas_no_db_error_and_success() {
        let ms = get_metastore().await;
        let db_name = "db_for_schemas_test_ok_sc".to_string(); // Unique name
        let schema_name = "schema_in_schemas_test_ok_sc".to_string();
        let schema_ident = SchemaIdent { database: db_name.clone(), schema: schema_name.clone() };
        let schema_payload = Schema { ident: schema_ident.clone(), properties: None };
        let no_db_result = ms.create_schema(&schema_ident, schema_payload.clone()).await;
        assert!(no_db_result.is_err());
        match no_db_result.unwrap_err() {
            MetastoreError::OperationFailed { source: emb_err } => {
                matches!(emb_err.source, MetastoreErrorKind::DatabaseNotFound { .. });
            }
            _ => panic!("Expected DatabaseNotFound error"),
        }
        insta::with_settings!({ filters => insta_filters(), }, {
            insta::assert_debug_snapshot!("create_schema_no_db_error_snapshot_sc", no_db_result.unwrap_err());
        });
        let volume = Volume::new("vol_for_schemas_test_ok_sc".to_owned(), VolumeType::Memory);
        assert!(ms.create_volume(&"vol_for_schemas_test_ok_sc".to_owned(), volume).await.is_ok());
        let database = Database { ident: db_name.clone(), volume: "vol_for_schemas_test_ok_sc".to_owned(), properties: None };
        assert!(ms.create_database(&db_name, database).await.is_ok());
        let create_res = ms.create_schema(&schema_ident, schema_payload.clone()).await;
        assert!(create_res.is_ok(), "Schema creation failed: {:?}", create_res.err());
        let list_res = ms.iter_schemas(&db_name).unwrap().collect::<MetastoreResult<Vec<_>>>().await;
        assert!(list_res.is_ok()); assert_eq!(list_res.unwrap().len(), 1);
        let get_res = ms.get_schema(&schema_ident).await;
        assert!(get_res.is_ok() && get_res.unwrap().is_some());
        assert!(ms.delete_schema(&schema_ident, false).await.is_ok());
        let list_after_delete_res = ms.iter_schemas(&db_name).unwrap().collect::<MetastoreResult<Vec<_>>>().await;
        assert!(list_after_delete_res.is_ok()); assert!(list_after_delete_res.unwrap().is_empty());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_tables_full_lifecycle_and_errors() {
        let ms = get_metastore().await;
        let db_name = "db_tbl_life".to_string();
        let schema_name = "sch_tbl_life".to_string();
        let table_name = "tbl_life".to_string();
        let table_ident = TableIdent { database: db_name.clone(), schema: schema_name.clone(), table: table_name.clone() };
        let iceberg_schema = IcebergSchema::builder().with_schema_id(0)
            .with_struct_field(StructField::new(0, "id", true, Type::Primitive(PrimitiveType::Int), None))
            .build().unwrap();
        let table_create_req = TableCreateRequest { ident: table_ident.clone(), schema: iceberg_schema.clone(), ..Default::default() };

        let no_schema_res = ms.create_table(&table_ident, table_create_req.clone()).await;
        assert!(no_schema_res.is_err()); // ... rest of assertion as before ...

        let vol = Volume::new("vol_tbl_life".to_owned(), VolumeType::Memory);
        ms.create_volume(&"vol_tbl_life".to_owned(), vol).await.expect("Setup");
        let db = Database { ident: db_name.clone(), volume: "vol_tbl_life".to_owned(), properties: None };
        ms.create_database(&db_name, db).await.expect("Setup");
        let sch = Schema { ident: SchemaIdent { database: db_name.clone(), schema: schema_name.clone() }, properties: None };
        ms.create_schema(&sch.ident, sch).await.expect("Setup");

        let create_res = ms.create_table(&table_ident, table_create_req.clone()).await;
        assert!(create_res.is_ok(), "Table create failed: {:?}", create_res.err());
        let created_rwo = create_res.unwrap();

        let obj_store_res = ms.table_object_store(&table_ident).await;
        assert!(obj_store_res.is_ok(), "{:?}", obj_store_res.err());
        let obj_store = obj_store_res.unwrap();
        let md_path = Path::from(url::Url::parse(&created_rwo.data.metadata_location).unwrap().path());
        assert!(obj_store.get(&md_path).await.is_ok());

        let list_res = ms.iter_tables(&table_ident.clone().into()).unwrap().collect::<MetastoreResult<Vec<_>>>().await;
        assert!(list_res.is_ok() && list_res.unwrap().len() == 1);
        let get_res = ms.get_table(&table_ident).await;
        assert!(get_res.is_ok() && get_res.unwrap().is_some());

        let non_existent_ident = TableIdent { table: "ghost".to_string(), ..table_ident.clone() };
        let get_ghost_res = ms.get_table(&non_existent_ident).await;
        assert!(get_ghost_res.is_ok() && get_ghost_res.unwrap().is_none());
        
        // Test delete with cascade=true to clean up object store data for this test
        let delete_res = ms.delete_table(&table_ident, true).await;
        assert!(delete_res.is_ok(), "Table delete failed: {:?}", delete_res.err());
        let list_after_delete = ms.iter_tables(&table_ident.into()).unwrap().collect::<MetastoreResult<Vec<_>>>().await;
        assert!(list_after_delete.is_ok() && list_after_delete.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_temporary_tables() {
        let ms = get_metastore().await;
        let db_name = "db_temp_tbl".to_string();
        let schema_name = "sch_temp_tbl".to_string();
        let table_name = "temp_tbl".to_string();
        let table_ident = TableIdent { database: db_name.clone(), schema: schema_name.clone(), table: table_name.clone() };

        let iceberg_schema = IcebergSchema::builder().with_schema_id(0)
            .with_struct_field(StructField::new(0,"id",true,Type::Primitive(PrimitiveType::Int),None)).build().unwrap();
        let table_req = TableCreateRequest {
            ident: table_ident.clone(), schema: iceberg_schema, is_temporary: Some(true), ..Default::default()
        };

        // Setup DB and Schema
        let vol = Volume::new("vol_temp_tbl".to_owned(), VolumeType::Memory);
        ms.create_volume(&"vol_temp_tbl".to_owned(), vol).await.expect("Setup");
        let db = Database { ident: db_name.clone(), volume: "vol_temp_tbl".to_owned(), properties: None };
        ms.create_database(&db_name, db).await.expect("Setup");
        let sch = Schema { ident: SchemaIdent{database: db_name.clone(), schema: schema_name.clone()}, properties: None };
        ms.create_schema(&sch.ident, sch).await.expect("Setup");

        // Create temporary table
        let create_res = ms.create_table(&table_ident, table_req.clone()).await;
        assert!(create_res.is_ok(), "Temporary table creation failed: {:?}", create_res.err());
        let created_table_rwo = create_res.unwrap();
        assert!(created_table_rwo.data.is_temporary);
        assert!(created_table_rwo.data.volume_ident.is_some(), "Temporary table should have an auto-created volume_ident");

        // Verify object store content for its metadata
        let obj_store_res = ms.table_object_store(&table_ident).await;
        assert!(obj_store_res.is_ok(), "{:?}", obj_store_res.err());
        let obj_store = obj_store_res.unwrap();
        let md_path = Path::from(url::Url::parse(&created_table_rwo.data.metadata_location).unwrap().path());
        let get_md_res = obj_store.get(&md_path).await;
        assert!(get_md_res.is_ok(), "Metadata for temporary table not found in its object store: {:?}", get_md_res.err());
        
        // Snapshot volume_ident and metadata path to check their structure (UUIDs are filtered by insta)
        insta::with_settings!({ filters => insta_filters(), }, {
            insta::assert_debug_snapshot!("temporary_table_details", (created_table_rwo.data.volume_ident.as_ref(), created_table_rwo.data.metadata_location));
        });

        // Delete the temporary table (cascade true to ensure volume cleanup)
        let delete_res = ms.delete_table(&table_ident, true).await;
        assert!(delete_res.is_ok(), "Deleting temporary table failed: {:?}", delete_res.err());

        // Verify the auto-created volume for the temporary table is also deleted
        if let Some(temp_vol_ident) = created_table_rwo.data.volume_ident {
            let get_temp_vol_res = ms.get_volume(&temp_vol_ident).await;
            assert!(get_temp_vol_res.is_ok() && get_temp_vol_res.unwrap().is_none(), 
                "Temporary volume '{}' should have been deleted with the table", temp_vol_ident);
        }
    }

    #[tokio::test]
    async fn test_update_table_requirement_failed() {
        let ms = get_metastore().await;
        // Setup: Create a table
        let db = "db_req_fail".to_string(); let sch = "sch_req_fail".to_string(); let tbl = "tbl_req_fail".to_string();
        let ident = TableIdent { database: db.clone(), schema: sch.clone(), table: tbl.clone() };
        let volume = Volume::new("vol_req_fail".to_string(), VolumeType::Memory);
        ms.create_volume(&"vol_req_fail".to_string(), volume).await.unwrap();
        ms.create_database(&db, Database { ident: db.clone(), volume: "vol_req_fail".to_string(), ..Default::default() }).await.unwrap();
        ms.create_schema(&SchemaIdent{database:db.clone(), schema:sch.clone()}, Schema{ident:SchemaIdent{database:db.clone(), schema:sch.clone()}, ..Default::default()}).await.unwrap();
        let iceberg_schema = IcebergSchema::builder().with_schema_id(0).with_struct_field(StructField::new(0,"id",true,Type::Primitive(PrimitiveType::Int),None)).build().unwrap();
        ms.create_table(&ident, TableCreateRequest{ident: ident.clone(), schema: iceberg_schema, ..Default::default()}).await.unwrap();

        // Attempt an update with a failing requirement (e.g., assert-table-uuid matches a wrong UUID)
        let update_req = TableUpdate {
            requirements: vec![TableRequirement::AssertTableUUID(Uuid::new_v4().to_string())], // This UUID won't match
            updates: vec![], // No actual updates, just the failing requirement
        };
        let result = ms.update_table(&ident, update_req).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            MetastoreError::OperationFailed { source: emb_err } => {
                match emb_err.source {
                    MetastoreErrorKind::TableRequirementFailed { message } => {
                        assert!(message.contains("Assertion failed: uuid must be"));
                    }
                    _ => panic!("Incorrect MetastoreErrorKind: {:?}", emb_err.source),
                }
            }
            _ => panic!("Incorrect MetastoreError variant"),
        }
    }

    #[tokio::test]
    async fn test_delete_schema_with_tables_no_cascade() {
        let ms = get_metastore().await;
        // Setup: Create schema with a table
        let db = "db_del_sch_cascade".to_string(); let sch = "sch_del_sch_cascade".to_string(); let tbl = "tbl_del_sch_cascade".to_string();
        let schema_ident = SchemaIdent{database:db.clone(), schema:sch.clone()};
        let table_ident = TableIdent { database: db.clone(), schema: sch.clone(), table: tbl.clone() };
        let volume = Volume::new("vol_del_sch_cascade".to_string(), VolumeType::Memory);
        ms.create_volume(&"vol_del_sch_cascade".to_string(), volume).await.unwrap();
        ms.create_database(&db, Database { ident: db.clone(), volume: "vol_del_sch_cascade".to_string(), ..Default::default() }).await.unwrap();
        ms.create_schema(&schema_ident, Schema{ident:schema_ident.clone(), ..Default::default()}).await.unwrap();
        let iceberg_schema = IcebergSchema::builder().with_schema_id(0).with_struct_field(StructField::new(0,"id",true,Type::Primitive(PrimitiveType::Int),None)).build().unwrap();
        ms.create_table(&table_ident, TableCreateRequest{ident: table_ident.clone(), schema: iceberg_schema, ..Default::default()}).await.unwrap();

        // Attempt to delete schema without cascade
        let result = ms.delete_schema(&schema_ident, false).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            MetastoreError::OperationFailed { source: emb_err } => {
                match emb_err.source {
                    MetastoreErrorKind::Validation { .. } => { // Placeholder, should be more specific like SchemaNotEmpty
                        assert!(emb_err.context.contains("contains tables") && emb_err.context.contains("cascade is false"));
                    }
                    _ => panic!("Incorrect MetastoreErrorKind: {:?}", emb_err.source),
                }
            }
            _ => panic!("Incorrect MetastoreError variant"),
        }
    }
}
