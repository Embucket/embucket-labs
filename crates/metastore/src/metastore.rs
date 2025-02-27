use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use iceberg::{spec::{FormatVersion, TableMetadataBuilder}, TableCreation};
use object_store::{path::Path, ObjectStore, PutPayload};
use serde::de::DeserializeOwned;
use snafu::ResultExt;
use tokio::sync::Mutex;
use icebucket_utils::Db;
use uuid::Uuid;
use crate::error::{self as metastore_error, MetastoreResult};

#[allow(clippy::wildcard_imports)]
use crate::models::*;

#[async_trait]
pub trait Metastore: Send + Sync {
    async fn list_volumes(&self) -> MetastoreResult<Vec<RwObject<IceBucketVolume>>>;
    async fn create_volume(&self, name: IceBucketVolumeIdent, volume: IceBucketVolume) -> MetastoreResult<RwObject<IceBucketVolume>>;
    async fn get_volume(&self, name: &IceBucketVolumeIdent) -> MetastoreResult<Option<RwObject<IceBucketVolume>>>;
    async fn update_volume(&self, name: &IceBucketVolumeIdent, volume: IceBucketVolume) -> MetastoreResult<RwObject<IceBucketVolume>>;
    async fn delete_volume(&self, name: &IceBucketVolumeIdent, cascade: bool) -> MetastoreResult<()>;
    async fn volume_object_store(&self, name: &IceBucketVolumeIdent) -> MetastoreResult<Arc<dyn ObjectStore>>;

    async fn list_databases(&self) -> MetastoreResult<Vec<RwObject<IceBucketDatabase>>>;
    async fn create_database(&self, name: IceBucketDatabaseIdent, database: IceBucketDatabase) -> MetastoreResult<RwObject<IceBucketDatabase>>;
    async fn get_database(&self, name: &IceBucketDatabaseIdent) -> MetastoreResult<Option<RwObject<IceBucketDatabase>>>;
    async fn update_database(&self, name: &IceBucketDatabaseIdent, database: IceBucketDatabase) -> MetastoreResult<RwObject<IceBucketDatabase>>;
    async fn delete_database(&self, name: &IceBucketDatabaseIdent, cascade: bool) -> MetastoreResult<()>;

    async fn list_schemas(&self, database: &IceBucketDatabaseIdent) -> MetastoreResult<Vec<RwObject<IceBucketSchema>>>;
    async fn create_schema(&self, ident: IceBucketSchemaIdent, schema: IceBucketSchema) -> MetastoreResult<RwObject<IceBucketSchema>>;
    async fn get_schema(&self, ident: &IceBucketSchemaIdent) -> MetastoreResult<Option<RwObject<IceBucketSchema>>>;
    async fn update_schema(&self, ident: &IceBucketSchemaIdent, schema: IceBucketSchema) -> MetastoreResult<RwObject<IceBucketSchema>>;
    async fn delete_schema(&self, ident: &IceBucketSchemaIdent, cascade: bool) -> MetastoreResult<()>;

    async fn list_tables(&self, schema: &IceBucketSchemaIdent) -> MetastoreResult<Vec<RwObject<IceBucketTable>>>;
    async fn create_table(&self, ident: IceBucketTableIdent, table: IceBucketTableCreateRequest) -> MetastoreResult<RwObject<IceBucketTable>>;
    async fn get_table(&self, ident: &IceBucketTableIdent) -> MetastoreResult<Option<RwObject<IceBucketTable>>>;
    async fn update_table(&self, ident: &IceBucketTableIdent, update: IceBucketTableUpdate) -> MetastoreResult<RwObject<IceBucketTable>>;
    async fn delete_table(&self, ident: &IceBucketTableIdent, cascade: bool) -> MetastoreResult<()>;
    async fn table_object_store(&self, ident: &IceBucketTableIdent) -> MetastoreResult<Arc<dyn ObjectStore>>;

    async fn url_for_table(&self, ident: &IceBucketTableIdent) -> MetastoreResult<String>;
    async fn volume_for_table(&self, ident: &IceBucketTableIdent) -> MetastoreResult<RwObject<IceBucketVolume>>;
}

///
/// volumes -> List of volumes
/// vol.<name> -> `IceBucketVolume`
/// databases -> List of databases
/// db.<name> -> `IceBucketDatabase`
/// schemas.<db> -> List of schemas for <db>
/// sch.<db>.<name> -> `IceBucketSchema`
/// tables.<db>.<schema> -> List of tables for <schema> in <db>
/// tbl.<db>.<schema>.<table> -> `IceBucketTable`
/// 
const KEY_VOLUMES: &str = "volumes";
const KEY_VOLUME: &str = "vol";
const KEY_DATABASES: &str = "databases";
const KEY_DATABASE: &str = "db";
const KEY_SCHEMAS: &str = "schemas";
const KEY_SCHEMA: &str = "sch";
const KEY_TABLES: &str = "tables";
const KEY_TABLE: &str = "tbl";

pub struct SlateDBMetastore {
    db: Db,
    object_store_cache: Mutex<HashMap<IceBucketVolumeIdent, Arc<dyn ObjectStore>>>,
}

impl SlateDBMetastore {
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self { db, object_store_cache: Mutex::new(HashMap::new()) }
    }

    #[cfg(test)]
    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }

    async fn list_objects<T>(&self, list_key: &str) -> MetastoreResult<Vec<RwObject<T>>>
    where
    T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync,
    {
        let keys = self.db.get::<Vec<String>>(list_key).await
            .context(metastore_error::UtilSlateDBSnafu)?
            .unwrap_or_default();
        let futures = keys.iter().map(|key| self.db.get(key)).collect::<Vec<_>>();
        let results = futures::future::try_join_all(futures).await
            .context(metastore_error::UtilSlateDBSnafu)?;
        let entities = results.into_iter().flatten().collect::<Vec<_>>();
        Ok(entities)
    }

    async fn create_object<T>(&self, key: &str, plural_key: &str, object_type: &str, object: T) -> MetastoreResult<RwObject<T>>
    where
        T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync,
    {
        if self.db.get::<T>(key).await
            .context(metastore_error::UtilSlateDBSnafu)?.is_none() {
            let rwobject = RwObject::new(object);
            self.db.put(key, &rwobject).await
                .context(metastore_error::UtilSlateDBSnafu)?;
            self.db.list_append(plural_key, key.to_string()).await
                .context(metastore_error::UtilSlateDBSnafu)?;
            Ok(rwobject)
        } else {
            Err(metastore_error::MetastoreError::ObjectAlreadyExists { type_name: object_type.to_owned(), name: key.to_string()})
        }
    }

    async fn update_object<T>(&self, key: &str, object: T) -> MetastoreResult<RwObject<T>>
    where
        T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync,
    {
        if let Some(mut rwo) = self.db.get::<RwObject<T>>(key).await
            .context(metastore_error::UtilSlateDBSnafu)? {
            rwo.update(object);
            self.db.put(key, &rwo).await
                .context(metastore_error::UtilSlateDBSnafu)?;
            Ok(rwo)
        } else {
            Err(metastore_error::MetastoreError::ObjectNotFound { type_name: "volume".to_owned(), name: key.to_string()})
        }
    }

    async fn delete_object(&self, key: &str, plural_key: &str) -> MetastoreResult<()>
    {
        self.db.delete(key).await.ok();
        self.db.list_remove(plural_key, key).await.ok();
        Ok(())
    }

    fn generate_metadata_filename() -> String {
        format!("{}.metadata.json", Uuid::new_v4())
    }

    #[allow(clippy::implicit_hasher)]
    pub fn update_properties_timestamps(properties: &mut HashMap<String, String>) {
        let utc_now = Utc::now();
        let utc_now_str = utc_now.to_rfc3339();
        properties.insert("created_at".to_string(), utc_now_str.clone());
        properties.insert("updated_at".to_string(), utc_now_str);
    }

    #[must_use]
    pub fn get_default_properties() -> HashMap<String, String> {
        let mut properties = HashMap::new();
        Self::update_properties_timestamps(&mut properties);
        properties
    }
}



#[async_trait]
impl Metastore for SlateDBMetastore {
    async fn list_volumes(&self) -> MetastoreResult<Vec<RwObject<IceBucketVolume>>> {
        self.list_objects(KEY_VOLUMES).await
    }

    async fn create_volume(&self, name: IceBucketVolumeIdent, volume: IceBucketVolume) -> MetastoreResult<RwObject<IceBucketVolume>> {
        let key = format!("{KEY_VOLUME}/{name}");
        let object_store = volume.get_object_store()?;
        let rwobject = self.create_object(&key, KEY_VOLUMES, "volume", volume).await?;
        let mut cache = self.object_store_cache.lock().await;
        cache.insert(name.clone(), object_store);
        Ok(rwobject)
    }

    async fn get_volume(&self, name: &IceBucketVolumeIdent) -> MetastoreResult<Option<RwObject<IceBucketVolume>>> {
        let key = format!("{KEY_VOLUME}/{name}");
        self.db.get(&key).await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    async fn update_volume(&self, name: &IceBucketVolumeIdent, volume: IceBucketVolume) -> MetastoreResult<RwObject<IceBucketVolume>> {
        let key = format!("{KEY_VOLUME}/{name}");
        self.update_object(&key, volume).await
    }

    async fn delete_volume(&self, name: &IceBucketVolumeIdent, cascade: bool) -> MetastoreResult<()> {
        let key = format!("{KEY_VOLUME}/{name}");
        let databases_using = self.list_databases().await?
            .into_iter()
            .filter(|db| db.volume == *name)
            .map(|db| db.ident.clone())
            .collect::<Vec<_>>();
        if cascade {
            let futures = databases_using
                .iter()
                .map(|db| self.delete_database(db, cascade)).collect::<Vec<_>>();
            futures::future::try_join_all(futures).await?;
            self.delete_object(&key, KEY_VOLUMES).await
        } else if databases_using.is_empty() {
            self.delete_object(&key, KEY_VOLUMES).await?;
            let mut cache = self.object_store_cache.lock().await;
            cache.remove(name);
            Ok(())
        } else {
            Err(
                metastore_error::MetastoreError::VolumeInUse { 
                    database: databases_using[..].join(", ")
                }
            )
        }
    }

    async fn volume_object_store(&self, name: &IceBucketVolumeIdent) -> MetastoreResult<Arc<dyn ObjectStore>> {
        let mut cache = self.object_store_cache.lock().await;
        if let Some(store) = cache.get(name) {
            Ok(store.clone())
        } else {
            let volume = self.get_volume(name).await?
                .ok_or(metastore_error::MetastoreError::ObjectNotFound { type_name: "volume".to_string(), name: name.clone() } )?;
            let object_store = volume.get_object_store()?;
            cache.insert(name.clone(), object_store.clone());
            Ok(object_store)
        }
    }

    async fn list_databases(&self) -> MetastoreResult<Vec<RwObject<IceBucketDatabase>>> {
        self.list_objects(KEY_DATABASES).await
    }

    async fn create_database(&self, name: IceBucketDatabaseIdent, database: IceBucketDatabase) -> MetastoreResult<RwObject<IceBucketDatabase>> {
        self.get_volume(&database.volume).await?
            .ok_or(metastore_error::MetastoreError::ObjectNotFound { type_name: "volume".to_string(), name: database.volume.clone() } )?;
        let key = format!("{KEY_DATABASE}/{name}");
        self.create_object(&key, KEY_DATABASES, "database", database).await
    }

    async fn get_database(&self, name: &IceBucketDatabaseIdent) -> MetastoreResult<Option<RwObject<IceBucketDatabase>>> {
        let key = format!("{KEY_DATABASE}/{name}");
        self.db.get(&key).await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    async fn update_database(&self, name: &IceBucketDatabaseIdent, database: IceBucketDatabase) -> MetastoreResult<RwObject<IceBucketDatabase>> {
        let key = format!("{KEY_DATABASE}/{name}");
        self.update_object(&key, database).await
    }

    async fn delete_database(&self, name: &IceBucketDatabaseIdent, cascade: bool) -> MetastoreResult<()> {
        let schemas = self.list_schemas(name).await?;
        if cascade {
            let futures = schemas
                .iter()
                .map(|schema| self.delete_schema(&schema.ident, cascade)
                ).collect::<Vec<_>>();
            futures::future::try_join_all(futures).await?; 
        }
        let key = format!("{KEY_DATABASE}/{name}");
        self.delete_object(&key, KEY_DATABASES).await
    }

    async fn list_schemas(&self, database: &IceBucketDatabaseIdent) -> MetastoreResult<Vec<RwObject<IceBucketSchema>>> {
        let key = format!("{KEY_SCHEMAS}/{database}");
        self.list_objects(&key).await
    }

    async fn create_schema(&self, ident: IceBucketSchemaIdent, schema: IceBucketSchema) -> MetastoreResult<RwObject<IceBucketSchema>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        if self.get_database(&ident.database).await?.is_some() {
            self.create_object(&key, &format!("{KEY_SCHEMAS}/{}", ident.database), "schema", schema).await
        } else {
            Err(metastore_error::MetastoreError::ObjectNotFound { type_name: "database".to_string(), name: ident.database })
        }
    }

    async fn get_schema(&self, ident: &IceBucketSchemaIdent) -> MetastoreResult<Option<RwObject<IceBucketSchema>>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.db.get(&key).await
            .context(metastore_error::UtilSlateDBSnafu)
    }
    
    async fn update_schema(&self, ident: &IceBucketSchemaIdent, schema: IceBucketSchema) -> MetastoreResult<RwObject<IceBucketSchema>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.update_object(&key, schema).await
    }

    async fn delete_schema(&self, ident: &IceBucketSchemaIdent, cascade: bool) -> MetastoreResult<()> {
        let tables = self.list_tables(ident).await?;

        if cascade {
            let futures = tables
                .iter()
                .map(|table| self.delete_table(&table.ident, cascade)
                ).collect::<Vec<_>>();
            futures::future::try_join_all(futures).await?;
        }
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        let plural_key = format!("{KEY_SCHEMAS}/{}", ident.database);
        self.delete_object(&key, &plural_key).await
    }

    async fn list_tables(&self, schema: &IceBucketSchemaIdent) -> MetastoreResult<Vec<RwObject<IceBucketTable>>> {
        let key = format!("{KEY_TABLES}/{}/{}", schema.database, schema.schema);
        self.list_objects(&key).await
    }

    async fn create_table(&self, ident: IceBucketTableIdent, table: IceBucketTableCreateRequest) -> MetastoreResult<RwObject<IceBucketTable>> {
        if let Some(_schema) = self.get_schema(&ident.clone().into()).await? {
            let key = format!("{KEY_TABLE}/{}/{}/{}", ident.database, ident.schema, ident.table);
            // create more stuff like the table metadata

            let metadata_part = format!("metadata/{}", Self::generate_metadata_filename());
            let table_location = self.url_for_table(&ident).await?;
            dbg!(&table_location);
            let table_creation = {
                let mut creation:TableCreation = table.clone().try_into()?;
                creation.location = Some(table_location.clone());
                creation
            };

            let table_metadata = TableMetadataBuilder::from_table_creation(table_creation)
                .and_then(|builder| builder.upgrade_format_version(FormatVersion::V2))
                .and_then(iceberg::spec::TableMetadataBuilder::build)
                .context(metastore_error::IcebergSnafu)?
                .metadata.clone();

            let mut table_properties = table.properties.unwrap_or_default().clone();
            Self::update_properties_timestamps(&mut table_properties);

            let table = IceBucketTable {
                ident: ident.clone(),
                metadata: table_metadata.clone(),
                metadata_location: format!("{table_location}/{metadata_part}"),
                properties: table_properties,
            };
            let rwo_table = self.create_object(&key, &format!("{KEY_TABLES}/{}/{}", ident.database, ident.schema), "table", table.clone()).await?;

            let object_store = self.table_object_store(&ident).await?;
            let data = Bytes::from(serde_json::to_vec(&table_metadata).context(metastore_error::SerdeSnafu)?);
            let path = Path::from(table.metadata_location.clone());
            let pr = object_store.put(&path, PutPayload::from(data))
                .await
                .context(metastore_error::ObjectStoreSnafu)?;
            dbg!(pr);
            Ok(rwo_table)
        } else {
            Err(metastore_error::MetastoreError::ObjectNotFound { type_name: "schema".to_string(), name: ident.to_string() })
        }
        
    }

    async fn update_table(&self, ident: &IceBucketTableIdent, update: IceBucketTableUpdate ) -> MetastoreResult<RwObject<IceBucketTable>> {
        let table = self.get_table(ident).await?
            .ok_or(metastore_error::MetastoreError::ObjectNotFound { type_name: "table".to_string(), name: ident.to_string() } )?;

        update.requirements
            .into_iter()
            .map(TableRequirementExt::new)
            .try_for_each(|req| req.assert(&table.metadata, true))?;

        let mut builder = TableMetadataBuilder::new_from_metadata(
            table.metadata.clone(), 
            Some(table.metadata_location.clone())
        );

        for update in update.updates {
            builder = update.apply(builder).context(metastore_error::IcebergSnafu)?;
        }
        let result = builder.build().context(metastore_error::IcebergSnafu)?;
        let table_metadata = result.metadata.clone();

        let mut properties = table.properties.clone();
        Self::update_properties_timestamps(&mut properties);

        let metadata_part = format!("metadata/{}", Self::generate_metadata_filename());
        let table_location = self.url_for_table(ident).await?;
        let metadata_location = format!("{table_location}/{metadata_part}");
        let table = IceBucketTable {
            ident: ident.clone(),
            metadata: table_metadata,
            metadata_location: metadata_location.clone(),
            properties,
        };

        let key = format!("{KEY_TABLE}/{}/{}/{}", ident.database, ident.schema, ident.table);
        let rw_table = self.update_object(&key, table.clone()).await?;

        let db = self.get_database(&ident.database).await?
                .ok_or(metastore_error::MetastoreError::ObjectNotFound { type_name: "database".to_string(), name: ident.database.clone() } )?;
        let volume = self.get_volume(&db.volume).await?
            .ok_or(metastore_error::MetastoreError::ObjectNotFound { type_name: "volume".to_string(), name: db.volume.clone() } )?;

        let object_store = volume.get_object_store()?;
        let data = Bytes::from(serde_json::to_vec(&table.metadata).context(metastore_error::SerdeSnafu)?);
        let path = Path::from(metadata_location);
        object_store.put(&path, PutPayload::from(data))
            .await
            .context(metastore_error::ObjectStoreSnafu)?;

        Ok(rw_table)
    }

    async fn delete_table(&self, ident: &IceBucketTableIdent, cascade: bool) -> MetastoreResult<()> {
        if self.get_table(ident).await?.is_some() {
            if cascade {
                let object_store = self.table_object_store(ident).await?;
                let metadata_path = Path::from(self.url_for_table(ident).await?);
                object_store.delete(&metadata_path).await
                    .context(metastore_error::ObjectStoreSnafu)?;
            }
            let key = format!("{KEY_TABLE}/{}/{}/{}", ident.database, ident.schema, ident.table);
            self.delete_object(&key, &format!("{KEY_TABLES}/{}/{}", ident.database, ident.schema)).await
        } else {
            Err(metastore_error::MetastoreError::ObjectNotFound { type_name: "table".to_string(), name: ident.to_string() })
        }
    }

    async fn get_table(&self, ident: &IceBucketTableIdent) -> MetastoreResult<Option<RwObject<IceBucketTable>>> {
        let key = format!("{KEY_TABLE}/{}/{}/{}", ident.database, ident.schema, ident.table);
        self.db.get(&key).await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    async fn table_object_store(&self, ident: &IceBucketTableIdent) -> MetastoreResult<Arc<dyn ObjectStore>> {
        let volume = self.volume_for_table(ident).await?;
        volume.get_object_store()
    }

    async fn url_for_table(&self, ident: &IceBucketTableIdent, ) -> MetastoreResult<String> {
        let database = self.get_database(&ident.database).await?
            .ok_or(metastore_error::MetastoreError::ObjectNotFound { type_name: "database".to_string(), name: ident.database.clone() } )?;

        let volume = self.get_volume(&database.volume).await?
            .ok_or(metastore_error::MetastoreError::ObjectNotFound { type_name: "volume".to_string(), name: database.volume.clone() } )?;

        let prefix = volume.prefix();
        Ok(format!("{}/{}/{}/{}", prefix, ident.database, ident.schema, ident.table))
    }

    async fn volume_for_table(&self, ident: &IceBucketTableIdent) -> MetastoreResult<RwObject<IceBucketVolume>> {
        let database = self.get_database(&ident.database).await?
            .ok_or(metastore_error::MetastoreError::ObjectNotFound { type_name: "database".to_string(), name: ident.database.clone() } )?;

        self.get_volume(&database.volume).await?
            .ok_or(metastore_error::MetastoreError::ObjectNotFound { type_name: "volume".to_string(), name: database.volume.clone() } )
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use either::Either;
    use futures::{StreamExt, TryStreamExt};
    use iceberg::spec::{NestedFieldRef, NestedField, Type, PrimitiveType};
    use object_store::ObjectMeta;
    use slatedb::db::Db as SlateDb;
    use std::sync::Arc;

    fn insta_filters() -> Vec<(&'static str, &'static str)> {
        vec![
            (r"created_at[^,]*", "created_at: \"TIMESTAMP\""),
            (r"updated_at[^,]*", "updated_at: \"TIMESTAMP\""),
        ]
    }

    async fn get_metastore() -> SlateDBMetastore {
        let object_store = object_store::memory::InMemory::new();
        let sdb = SlateDb::open(Path::from("/"), Arc::new(object_store)).await.expect("Failed to open db");
        let db = Db::new(Arc::new(sdb));
        SlateDBMetastore::new(db)   
    }

    #[tokio::test]
    async fn test_create_volumes() {
        let ms = get_metastore().await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume("test".to_owned(), volume).await.expect("create volume failed");
        let all_volumes = ms.list_volumes().await.expect("list volumes failed");

        let volumes_key = ms.db().get::<Vec<String>>(KEY_VOLUMES)
            .await
            .expect("get volumes key failed")
            .unwrap_or_default();
        let test_volume = ms.db().get::<serde_json::Value>(&format!("{KEY_VOLUME}/test")).await.expect("get test volume failed");
        
        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((volumes_key, test_volume, all_volumes));
        });
    }

    #[tokio::test]
    async fn test_duplicate_volume() {
        let ms = get_metastore().await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume("test".to_owned(), volume).await.expect("create volume failed");

        let volume2 = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        let result = ms.create_volume("test".to_owned(), volume2).await;
        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!(result);
        });
    }

    #[tokio::test]
    async fn test_delete_volume() {
        let ms = get_metastore().await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume("test".to_string(), volume).await.expect("create volume failed");
        let all_volumes = ms.list_volumes().await.expect("list volumes failed");
        ms.delete_volume(&"test".to_string(), false).await.expect("delete volume failed");
        let all_volumes_after = ms.list_volumes().await.expect("list volumes failed");
        let get_volume = ms.get_volume(&"test".to_owned()).await.expect("get volume failed");

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((all_volumes, all_volumes_after, get_volume));
        });

    }

    #[tokio::test]
    async fn test_update_volume() {
        let ms = get_metastore().await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        let rwo1 = ms.create_volume("test".to_owned(), volume).await.expect("create volume failed");
        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::File(IceBucketFileVolume { path: "/tmp".to_owned() }));
        let rwo2 = ms.update_volume(&"test".to_owned(), volume).await.expect("update volume failed");
        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((rwo1, rwo2));
        });   
    }

    #[tokio::test]
    async fn test_create_database() {
        let ms = get_metastore().await;
        let mut database = IceBucketDatabase {
            ident: "testdb".to_owned(),
            volume: "testv1".to_owned(),
            properties: None,
        };
        let no_volume_result = ms.create_database("testdb".to_owned(), database.clone()).await;
        
        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        let volume2 = IceBucketVolume::new("test2".to_owned(), IceBucketVolumeType::File(IceBucketFileVolume { path: "/tmp".to_owned() }));
        ms.create_volume("testv1".to_owned(), volume).await.expect("create volume failed");
        ms.create_volume("testv2".to_owned(), volume2).await.expect("create volume failed");
        ms.create_database("testdb".to_owned(), database.clone()).await.expect("create database failed");
        let all_databases = ms.list_databases().await.expect("list databases failed");

        database.volume = "testv2".to_owned();
        ms.update_database(&"testdb".to_owned(), database).await.expect("update database failed");
        let fetched_db = ms.get_database(&"testdb".to_owned()).await.expect("get database failed");

        ms.delete_database(&"testdb".to_string(), false).await.expect("delete database failed");
        let all_dbs_after = ms.list_databases().await.expect("list databases failed");
        
        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((no_volume_result, all_databases, fetched_db, all_dbs_after));
        });
    }

    #[tokio::test]
    async fn test_schemas() {
        let ms = get_metastore().await;
        let schema = IceBucketSchema {
            ident: IceBucketSchemaIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
            },
            properties: None,
        };

        let no_db_result = ms.create_schema(schema.ident.clone(), schema.clone()).await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume("testv1".to_owned(), volume).await.expect("create volume failed");
        ms.create_database("testdb".to_owned(), IceBucketDatabase { ident: "testdb".to_owned(), volume: "testv1".to_owned(), properties: None }).await.expect("create database failed");
        let schema_create = ms.create_schema(schema.ident.clone(), schema.clone()).await.expect("create schema failed");

        let schema_list = ms.list_schemas(&schema.ident.database).await.expect("list schemas failed");
        let schema_get = ms.get_schema(&schema.ident).await.expect("get schema failed");
        ms.delete_schema(&schema.ident, false).await.expect("delete schema failed");
        let schema_list_after = ms.list_schemas(&schema.ident.database).await.expect("list schemas failed");

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((no_db_result, schema_create, schema_list, schema_get, schema_list_after));
        });
    }

    #[tokio::test]
    async fn test_tables() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let sdb = SlateDb::open(Path::from("/"), object_store.clone()).await.expect("Failed to open db");
        let db = Db::new(Arc::new(sdb));
        let ms = SlateDBMetastore::new(db);

        let schema_fields = vec![
            NestedFieldRef::new(NestedField::new(
                    0,
                    "id".to_owned(),
                    Type::Primitive(PrimitiveType::Int),
                    false
            )),
            NestedFieldRef::new(NestedField::new(
                1,
                "name".to_owned(),
                Type::Primitive(PrimitiveType::String),
                false
        )),
        ];

        let table = IceBucketTableCreateRequest {
            ident: IceBucketTableIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
                table: "testtable".to_owned(),
            },
            format: None,
            properties: None,
            location: None,
            schema: Either::Left(IceBucketSimpleSchema {
                fields: schema_fields,
                schema_id: None,
            }),
            partition_spec: None,
            write_order: None,
            stage_create: None,
        };

        let no_schema_result = ms.create_table(table.ident.clone(), table.clone()).await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume("testv1".to_owned(), volume).await.expect("create volume failed");
        ms.create_database("testdb".to_owned(), IceBucketDatabase { ident: "testdb".to_owned(), volume: "testv1".to_owned(), properties: None }).await.expect("create database failed");
        ms.create_schema(IceBucketSchemaIdent { database: "testdb".to_owned(), schema: "testschema".to_owned() }, IceBucketSchema { ident: IceBucketSchemaIdent { database: "testdb".to_owned(), schema: "testschema".to_owned() }, properties: None }).await.expect("create schema failed");
        let table_create = ms.create_table(table.ident.clone(), table.clone()).await.expect("create table failed");
        let vol_object_store = ms.volume_object_store(&"testv1".to_owned()).await.expect("get volume object store failed");
        let paths: Result<Vec<_>, ()> = vol_object_store.list(None)
            .then(|c| async move { Ok::<_, ()>(c) })
            .collect::<Vec<Result<_, _>>>()
            .await
            .into_iter()
            .collect(); 

        let table_list = ms.list_tables(&table.ident.clone().into()).await.expect("list tables failed");
        let table_get = ms.get_table(&table.ident).await.expect("get table failed");
        ms.delete_table(&table.ident, false).await.expect("delete table failed");
        let table_list_after = ms.list_tables(&table.ident.into()).await.expect("list tables failed");

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((no_schema_result, table_create, paths, table_list, table_get, table_list_after));
        });
    }
}