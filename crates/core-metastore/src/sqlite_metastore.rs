use std::{collections::HashMap, sync::Arc};

#[allow(clippy::wildcard_imports)]
use crate::models::*;
use crate::{
    Metastore,
    error::{self as metastore_err, Result},
    models::{
        RwObject,
        database::{Database, DatabaseIdent},
        schema::{Schema, SchemaIdent},
        table::{Table, TableCreateRequest, TableIdent, TableRequirementExt, TableUpdate},
        volumes::{Volume, VolumeIdent},
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use core_utils::Db;
use core_utils::scan_iterator::{ScanIterator, VecScanIterator};
use diesel::{migration, migration::MigrationVersion};
use rusqlite::Result as SqlResult;
use dashmap::DashMap;
use futures::{StreamExt, TryStreamExt};
use iceberg_rust::catalog::commit::{TableUpdate as IcebergTableUpdate, apply_table_updates};
use iceberg_rust_spec::{
    schema::Schema as IcebergSchema,
    table_metadata::{FormatVersion, TableMetadataBuilder},
    types::StructField,
};
use object_store::{ObjectStore, PutPayload, path::Path};
use serde::de::DeserializeOwned;
use snafu::ResultExt;
use strum::Display;
use tracing::instrument;
use uuid::Uuid;
use core_sqlite::SqliteDb;

use deadpool_diesel::sqlite::{Manager, Pool as DieselPool, Runtime};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use crate::sqlite::crud;

pub const SQLITE_METASTORE_DB_NAME: &str = "sqlite_data/metastore.db";

pub const EMBED_MIGRATIONS: EmbeddedMigrations = embed_migrations!("src/sqlite/migrations");

// const METASTORE_TABLES_CREATE_TABLE: &str = "
// CREATE TABLE IF NOT EXISTS tables (
//     ident TEXT PRIMARY KEY,              -- Table identifier (UUID or unique string)
//     name TEXT NOT NULL,                  -- Table name
//     metadata TEXT NOT NULL,              -- JSON/text representation of TableMetadata
//     metadata_location TEXT NOT NULL,     -- File or object store path
//     properties TEXT,                     -- Serialized key/value map (JSON)
//     volume_ident TEXT,                   -- Optional UUID or string
//     volume_location TEXT,                -- Optional path
//     is_temporary INTEGER NOT NULL,       -- 0 or 1 (SQLite doesn’t have real BOOLEAN)
//     format TEXT NOT NULL                 -- TableFormat enum as TEXT (parquet, csv, etc.)
// );";


#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[strum(serialize_all = "lowercase")]
pub enum MetastoreObjectType {
    Volume,
    Database,
    Schema,
    Table,
}


///
/// vol -> List of volumes
/// vol/<name> -> `Volume`
/// db -> List of databases
/// db/<name> -> `Database`
/// sch/<db> -> List of schemas for <db>
/// sch/<db>/<name> -> `Schema`
/// tbl/<db>/<schema> -> List of tables for <schema> in <db>
/// tbl/<db>/<schema>/<table> -> `Table`
///
const KEY_VOLUME: &str = "vol";
const KEY_DATABASE: &str = "db";
const KEY_SCHEMA: &str = "sch";
const KEY_TABLE: &str = "tbl";

pub struct SlateDBMetastore {
    db: Db,
    object_store_cache: DashMap<VolumeIdent, Arc<dyn ObjectStore>>,
    pub diesel_pool: DieselPool,
}

impl std::fmt::Debug for SlateDBMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBMetastore").finish()
    }
}

impl SlateDBMetastore {
    #[allow(clippy::expect_used)]
    pub async fn new(db: core_utils::Db) -> Result<Self> {
        if let Some(dir_path) = std::path::Path::new(SQLITE_METASTORE_DB_NAME).parent() {
            std::fs::create_dir_all(dir_path).context(metastore_err::CreateDirSnafu)?;
        }

        // use this machinery just to set pragmas
        let _ = SqliteDb::new(db.slate_db(), SQLITE_METASTORE_DB_NAME)
            .await
            .context(metastore_err::CoreSqliteSnafu)?;

        let metastore = Self {
            //
            db: db.clone(), // to be removed
            object_store_cache: DashMap::new(),  // to be removed
            //
            diesel_pool: Self::create_pool(SQLITE_METASTORE_DB_NAME).await?,
        };
        metastore.create_tables().await?;
        Ok(metastore)
    }

   // Create a new store with a new in-memory database
    #[allow(clippy::expect_used)]
    pub async fn new_in_memory() -> Self {       
        let utils_db = core_utils::Db::memory().await;

        // use unique filename for every test, create in memory database
        let thread = std::thread::current();
        let thread_name = thread
            .name()
            .map_or("<unnamed>", |s| s.split("::").last().unwrap_or("<unnamed>"));
        let sqlite_db_name = format!("file:{thread_name}_meta?mode=memory");
        let _ = SqliteDb::new(utils_db.slate_db(), &sqlite_db_name)
            .await
            .expect("Failed to create Sqlite Db for metastore");        
        let store = Self {
            //
            db: utils_db.clone(), // to be removed
            object_store_cache: DashMap::new(),  // to be removed
            //
            diesel_pool: Self::create_pool(&sqlite_db_name)
                .await
                .expect("Failed to create Diesel Pool for metastore"),
        };

        store
            .create_tables()
            .await
            .expect("Failed to create tables");
        store
    }

    pub async fn create_pool(conn_str: &str) -> Result<DieselPool> {
        let pool = DieselPool::builder(
            Manager::new(
                conn_str, 
                Runtime::Tokio1)
            )
            .max_size(8)
            .build()
            .context(metastore_err::BuildPoolSnafu)?;
        Ok(pool)
    }

   #[instrument(
        name = "SqliteSqliteMetastore::create_tables",
        level = "debug",
        skip(self),
        fields(ok),
        err
    )]
    pub async fn create_tables(&self) -> Result<()> {
        let conn = self.diesel_pool.get()
            .await
            .context(metastore_err::DieselPoolSnafu)?;
        
        let migrations = conn.interact(|conn| -> migration::Result<Vec<String>> {
            Ok(conn.run_pending_migrations(EMBED_MIGRATIONS)?.iter().map(|m| m.to_string()).collect::<Vec<String>>())
        })
        .await?
        .context(metastore_err::GenericSnafu)?;

        tracing::info!("create_tables using migrations: {migrations:?}");
        Ok(())
    }

    #[cfg(test)]
    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }

    fn iter_objects<T>(&self, iter_key: String) -> VecScanIterator<RwObject<T>>
    where
        T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync,
    {
        self.db.iter_objects(iter_key)
    }

    #[instrument(
        name = "SlateDBSqliteMetastore::create_object",
        level = "debug",
        skip(self, object),
        err
    )]
    async fn create_object<T>(
        &self,
        key: &str,
        object_type: MetastoreObjectType,
        object: T,
    ) -> Result<RwObject<T>>
    where
        T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync,
    {
        if self
            .db
            .get::<RwObject<T>>(key)
            .await
            .context(metastore_err::UtilSlateDBSnafu)?
            .is_none()
        {
            let rwobject = RwObject::new(object);
            self.db
                .put(key, &rwobject)
                .await
                .context(metastore_err::UtilSlateDBSnafu)?;
            Ok(rwobject)
        } else {
            Err(metastore_err::ObjectAlreadyExistsSnafu {
                type_name: object_type.to_string(),
                name: key.to_string(),
            }
            .build())
        }
    }

    #[instrument(
        name = "SlateDBSqliteMetastore::update_object",
        level = "debug",
        skip(self, object),
        err
    )]
    async fn update_object<T>(&self, key: &str, object: T) -> Result<RwObject<T>>
    where
        T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync,
    {
        if let Some(mut rwo) = self
            .db
            .get::<RwObject<T>>(key)
            .await
            .context(metastore_err::UtilSlateDBSnafu)?
        {
            rwo.update(object);
            self.db
                .put(key, &rwo)
                .await
                .context(metastore_err::UtilSlateDBSnafu)?;
            Ok(rwo)
        } else {
            Err(metastore_err::ObjectNotFoundSnafu {}.build())
        }
    }

    #[instrument(
        name = "SlateDBSqliteMetastore::delete_object",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_object(&self, key: &str) -> Result<()> {
        self.db.delete(key).await.ok();
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


    // #[instrument(
    //     name = "SlateDBSqliteMetastore::create_object",
    //     level = "debug",
    //     skip(self, object),
    //     err
    // )]
    // async fn create_object<T>(
    //     &self,
    //     key: &str,
    //     object_type: MetastoreObjectType,
    //     object: T,
    // ) -> Result<RwObject<T>> {

    // }
}

#[async_trait]
impl Metastore for SlateDBMetastore {
    #[instrument(
        name = "SqliteMetastore::get_volumes",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_volumes(&self) -> Result<Vec<RwObject<Volume>>> {
        crud::volumes::list_volumes(&self.diesel_pool).await
    }

    #[instrument(
        name = "SqliteMetastore::create_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn create_volume(&self, volume: Volume) -> Result<RwObject<Volume>> {
        // let key = format!("{KEY_VOLUME}/{}", volume.ident);
        let object_store = volume.get_object_store()?;

        let rwobject = RwObject::new(volume);
        let inserted_count = crud::volumes::create_volume(&self.diesel_pool, rwobject.clone())
            .await?;

        tracing::debug!("Volume {} created, rows inserted {inserted_count}", rwobject.ident);

        // let rwobject = self
        //     .create_object(&key, MetastoreObjectType::Volume, volume.clone())
        //     .await
        //     .map_err(|e| {
        //         if matches!(e, metastore_err::Error::ObjectAlreadyExists { .. }) {
        //             metastore_err::VolumeAlreadyExistsSnafu {
        //                 volume: volume.ident.clone(),
        //             }
        //             .build()
        //         } else {
        //             e
        //         }
        //     })?;
        self.object_store_cache.insert(rwobject.ident.clone(), object_store);
        Ok(rwobject)
    }

    #[instrument(name = "SqliteMetastore::get_volume", level = "trace", skip(self), err)]
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>> {
        crud::volumes::get_volume(&self.diesel_pool, name).await
    }

    // TODO: Allow rename only here or on REST API level 
    #[instrument(
        name = "SqliteMetastore::update_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn update_volume(&self, ident: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        let key = format!("{KEY_VOLUME}/{ident}");
        let updated_volume = self.update_object(&key, volume).await?;
        let object_store = updated_volume.get_object_store()?;
        if ident != &updated_volume.ident {
            // object store cache is by name, so delete old name and add new
            self.object_store_cache.remove(ident);
            self.object_store_cache.insert(updated_volume.ident.clone(), object_store);
        } else {
            self.object_store_cache
                .alter(&updated_volume.ident, |_, _store| object_store.clone());
        }
        Ok(updated_volume)
    }

    #[instrument(name = "SqliteMetastore::delete_volume", level = "debug", skip(self), err)]
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()> {
        crud::volumes::delete_volume(&self.diesel_pool, name).await?;
        
        let key = format!("{KEY_VOLUME}/{name}");
        let databases_using = self
            .iter_databases()
            .collect()
            .await
            .context(metastore_err::UtilSlateDBSnafu)?
            .into_iter()
            .filter(|db| db.volume == *name)
            .map(|db| db.ident.clone())
            .collect::<Vec<_>>();
        if cascade {
            let futures = databases_using
                .iter()
                .map(|db| self.delete_database(db, cascade))
                .collect::<Vec<_>>();
            futures::future::try_join_all(futures).await?;
            self.delete_object(&key).await
        } else if databases_using.is_empty() {
            self.delete_object(&key).await?;
            self.object_store_cache.remove(name);
            Ok(())
        } else {
            Err(metastore_err::VolumeInUseSnafu {
                database: databases_using[..].join(", "),
            }
            .build())
        }
    }

    #[instrument(
        name = "SqliteMetastore::volume_object_store",
        level = "trace",
        skip(self),
        err
    )]
    async fn volume_object_store(
        &self,
        name: &VolumeIdent,
    ) -> Result<Option<Arc<dyn ObjectStore>>> {
        if let Some(store) = self.object_store_cache.get(name) {
            Ok(Some(store.clone()))
        } else {
            let volume = self.get_volume(name).await?.ok_or_else(|| {
                metastore_err::VolumeNotFoundSnafu {
                    volume: name.clone(),
                }
                .build()
            })?;
            let object_store = volume.get_object_store()?;
            self.object_store_cache
                .insert(name.clone(), object_store.clone());
            Ok(Some(object_store))
        }
    }

    #[instrument(name = "SqliteMetastore::iter_databases", level = "trace", skip(self))]
    fn iter_databases(&self) -> VecScanIterator<RwObject<Database>> {
        self.iter_objects(KEY_DATABASE.to_string())
    }

    #[instrument(
        name = "SqliteMetastore::create_database",
        level = "debug",
        skip(self, database),
        err
    )]
    async fn create_database(
        &self,
        database: Database,
    ) -> Result<RwObject<Database>> {
        self.get_volume(&database.volume).await?.ok_or_else(|| {
            metastore_err::VolumeNotFoundSnafu {
                volume: database.volume.clone(),
            }
            .build()
        })?;
        let key = format!("{KEY_DATABASE}/{}", database.ident);
        self.create_object(&key, MetastoreObjectType::Database, database)
            .await
    }

    #[instrument(name = "SqliteMetastore::get_database", level = "trace", skip(self), err)]
    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>> {
        let key = format!("{KEY_DATABASE}/{name}");
        self.db
            .get(&key)
            .await
            .context(metastore_err::UtilSlateDBSnafu)
    }

    #[instrument(
        name = "SqliteMetastore::update_database",
        level = "debug",
        skip(self, database),
        err
    )]
    async fn update_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>> {
        let key = format!("{KEY_DATABASE}/{name}");
        self.update_object(&key, database).await
    }

    #[instrument(name = "SqliteMetastore::delete_database", level = "debug", skip(self), err)]
    async fn delete_database(&self, name: &str, cascade: bool) -> Result<()> {
        let schemas = self
            .iter_schemas(name)
            .collect()
            .await
            .context(metastore_err::UtilSlateDBSnafu)?;
        if cascade {
            let futures = schemas
                .iter()
                .map(|schema| self.delete_schema(&schema.ident, cascade))
                .collect::<Vec<_>>();
            futures::future::try_join_all(futures).await?;
        } else if !schemas.is_empty() {
            return Err(metastore_err::DatabaseInUseSnafu {
                database: name,
                schema: schemas
                    .iter()
                    .map(|s| s.ident.schema.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            }
            .build());
        }
        let key = format!("{KEY_DATABASE}/{name}");
        self.delete_object(&key).await
    }
    #[instrument(name = "SqliteMetastore::iter_schemas", level = "debug", skip(self))]
    fn iter_schemas(&self, database: &str) -> VecScanIterator<RwObject<Schema>> {
        //If database is empty, we are iterating over all schemas
        let key = if database.is_empty() {
            KEY_SCHEMA.to_string()
        } else {
            format!("{KEY_SCHEMA}/{database}")
        };
        self.iter_objects(key)
    }

    #[instrument(
        name = "SqliteMetastore::create_schema",
        level = "debug",
        skip(self, schema),
        err
    )]
    async fn create_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        if self.get_database(&ident.database).await?.is_some() {
            self.create_object(&key, MetastoreObjectType::Schema, schema)
                .await
        } else {
            Err(metastore_err::DatabaseNotFoundSnafu {
                db: ident.database.clone(),
            }
            .build())
        }
    }

    #[instrument(name = "SqliteMetastore::get_schema", level = "debug", skip(self), err)]
    async fn get_schema(&self, ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.db
            .get(&key)
            .await
            .context(metastore_err::UtilSlateDBSnafu)
    }

    #[instrument(
        name = "SqliteMetastore::update_schema",
        level = "debug",
        skip(self, schema),
        err
    )]
    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.update_object(&key, schema).await
    }

    #[instrument(name = "SqliteMetastore::delete_schema", level = "debug", skip(self), err)]
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> Result<()> {
        let tables = self
            .iter_tables(ident)
            .collect()
            .await
            .context(metastore_err::UtilSlateDBSnafu)?;
        if cascade {
            let futures = tables
                .iter()
                .map(|table| self.delete_table(&table.ident, cascade))
                .collect::<Vec<_>>();
            futures::future::try_join_all(futures).await?;
        }
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.delete_object(&key).await
    }

    #[instrument(name = "SqliteMetastore::iter_tables", level = "debug", skip(self))]
    fn iter_tables(&self, schema: &SchemaIdent) -> VecScanIterator<RwObject<Table>> {
        //If database and schema is empty, we are iterating over all tables
        let key = if schema.schema.is_empty() && schema.database.is_empty() {
            KEY_TABLE.to_string()
        } else {
            format!("{KEY_TABLE}/{}/{}", schema.database, schema.schema)
        };
        self.iter_objects(key)
    }

    #[allow(clippy::too_many_lines)]
    #[instrument(name = "SqliteMetastore::create_table", level = "debug", skip(self), err)]
    async fn create_table(
        &self,
        ident: &TableIdent,
        mut table: TableCreateRequest,
    ) -> Result<RwObject<Table>> {
        if let Some(_schema) = self.get_schema(&ident.clone().into()).await? {
            let key = format!(
                "{KEY_TABLE}/{}/{}/{}",
                ident.database, ident.schema, ident.table
            );

            // This is duplicating the behavior of url_for_table,
            // but since the table won't exist yet we have to create it here
            let table_location = if table.is_temporary.unwrap_or_default() {
                let volume_ident: String = table.volume_ident.as_ref().map_or_else(
                    || Uuid::new_v4().to_string(),
                    std::string::ToString::to_string,
                );
                let volume = Volume {
                    ident: volume_ident.clone(),
                    volume: VolumeType::Memory,
                };
                let volume = self.create_volume(volume).await?;
                if table.volume_ident.is_none() {
                    table.volume_ident = Some(volume_ident);
                }

                table.location.as_ref().map_or_else(
                    || volume.prefix(),
                    |volume_location| format!("{}/{volume_location}", volume.prefix()),
                )
            } else {
                let database = self.get_database(&ident.database).await?.ok_or_else(|| {
                    metastore_err::DatabaseNotFoundSnafu {
                        db: ident.database.clone(),
                    }
                    .build()
                })?;
                let volume = self.get_volume(&database.volume).await?.ok_or_else(|| {
                    metastore_err::VolumeNotFoundSnafu {
                        volume: database.volume.clone(),
                    }
                    .build()
                })?;
                if table.volume_ident.is_none() {
                    table.volume_ident = Some(database.volume.clone());
                }

                let schema = url_encode(&ident.schema);
                let table = url_encode(&ident.table);

                let prefix = volume.prefix();
                format!("{prefix}/{}/{}/{}", ident.database, schema, table)
            };

            let metadata_part = format!("metadata/{}", Self::generate_metadata_filename());

            let mut table_metadata = TableMetadataBuilder::default();

            let schema = convert_schema_fields_to_lowercase(&table.schema)?;

            table_metadata
                .current_schema_id(*table.schema.schema_id())
                .with_schema((0, schema))
                .format_version(FormatVersion::V2);

            if let Some(properties) = table.properties.as_ref() {
                table_metadata.properties(properties.clone());
            }

            if let Some(partitioning) = table.partition_spec {
                table_metadata.with_partition_spec((0, partitioning));
            }

            if let Some(sort_order) = table.sort_order {
                table_metadata.with_sort_order((0, sort_order));
            }

            if let Some(location) = &table.location {
                table_metadata.location(location.clone());
            } else {
                table_metadata.location(table_location.clone());
            }

            let table_format = table.format.unwrap_or(TableFormat::Iceberg);

            let table_metadata = table_metadata
                .build()
                .context(metastore_err::TableMetadataBuilderSnafu)?;

            let mut table_properties = table.properties.unwrap_or_default().clone();
            Self::update_properties_timestamps(&mut table_properties);

            let table = Table {
                ident: ident.clone(),
                metadata: table_metadata.clone(),
                metadata_location: format!("{table_location}/{metadata_part}"),
                properties: table_properties,
                volume_ident: table.volume_ident,
                volume_location: table.location,
                is_temporary: table.is_temporary.unwrap_or_default(),
                format: table_format,
            };
            let rwo_table = self
                .create_object(&key, MetastoreObjectType::Table, table.clone())
                .await?;

            let object_store = self.table_object_store(ident).await?.ok_or_else(|| {
                metastore_err::TableObjectStoreNotFoundSnafu {
                    table: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
            let data = Bytes::from(
                serde_json::to_vec(&table_metadata).context(metastore_err::SerdeSnafu)?,
            );

            let url = url::Url::parse(&table.metadata_location)
                .context(metastore_err::UrlParseSnafu)?;
            let path = Path::from(url.path());
            object_store
                .put(&path, PutPayload::from(data))
                .await
                .context(metastore_err::ObjectStoreSnafu)?;
            Ok(rwo_table)
        } else {
            Err(metastore_err::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .build())
        }
    }

    #[instrument(
        name = "SqliteMetastore::update_table",
        level = "debug",
        skip(self, update),
        err
    )]
    async fn update_table(
        &self,
        ident: &TableIdent,
        mut update: TableUpdate,
    ) -> Result<RwObject<Table>> {
        let mut table = self
            .get_table(ident)
            .await?
            .ok_or_else(|| {
                metastore_err::TableNotFoundSnafu {
                    table: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?
            .data;

        update
            .requirements
            .into_iter()
            .map(TableRequirementExt::new)
            .try_for_each(|req| req.assert(&table.metadata))?;

        convert_add_schema_update_to_lowercase(&mut update.updates)?;

        apply_table_updates(&mut table.metadata, update.updates)
            .context(metastore_err::IcebergSnafu)?;

        let mut properties = table.properties.clone();
        Self::update_properties_timestamps(&mut properties);

        let metadata_part = format!("metadata/{}", Self::generate_metadata_filename());
        let table_location = self.url_for_table(ident).await?;
        let metadata_location = format!("{table_location}/{metadata_part}");

        table.metadata_location = String::from(&metadata_location);

        let key = format!(
            "{KEY_TABLE}/{}/{}/{}",
            ident.database, ident.schema, ident.table
        );
        let rw_table = self.update_object(&key, table.clone()).await?;

        let db = self.get_database(&ident.database).await?.ok_or_else(|| {
            metastore_err::DatabaseNotFoundSnafu {
                db: ident.database.clone(),
            }
            .build()
        })?;
        let volume = self.get_volume(&db.volume).await?.ok_or_else(|| {
            metastore_err::VolumeNotFoundSnafu {
                volume: db.volume.clone(),
            }
            .build()
        })?;

        let object_store = volume.get_object_store()?;
        let data =
            Bytes::from(serde_json::to_vec(&table.metadata).context(metastore_err::SerdeSnafu)?);

        let url = url::Url::parse(&metadata_location).context(metastore_err::UrlParseSnafu)?;
        let path = Path::from(url.path());

        object_store
            .put(&path, PutPayload::from(data))
            .await
            .context(metastore_err::ObjectStoreSnafu)?;

        Ok(rw_table)
    }

    #[instrument(name = "SqliteMetastore::delete_table", level = "debug", skip(self), err)]
    async fn delete_table(&self, ident: &TableIdent, cascade: bool) -> Result<()> {
        if let Some(table) = self.get_table(ident).await? {
            if cascade {
                let object_store = self.table_object_store(ident).await?.ok_or_else(|| {
                    metastore_err::TableObjectStoreNotFoundSnafu {
                        table: ident.table.clone(),
                        schema: ident.schema.clone(),
                        db: ident.database.clone(),
                    }
                    .build()
                })?;
                let url = url::Url::parse(&self.url_for_table(ident).await?)
                    .context(metastore_err::UrlParseSnafu)?;
                let metadata_path = Path::from(url.path());

                // List object
                let locations = object_store
                    .list(Some(&metadata_path))
                    .map_ok(|m| m.location)
                    .boxed();
                // Delete them
                object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await
                    .context(metastore_err::ObjectStoreSnafu)?;
            }

            if table.is_temporary {
                let volume_ident = table.volume_ident.as_ref().map_or_else(
                    || Uuid::new_v4().to_string(),
                    std::string::ToString::to_string,
                );
                self.delete_volume(&volume_ident, false).await?;
            }
            let key = format!(
                "{KEY_TABLE}/{}/{}/{}",
                ident.database, ident.schema, ident.table
            );
            self.delete_object(&key).await
        } else {
            Err(metastore_err::TableNotFoundSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .build())
        }
    }

    #[instrument(name = "SqliteMetastore::get_table", level = "debug", skip(self))]
    async fn get_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Table>>> {
        let key = format!(
            "{KEY_TABLE}/{}/{}/{}",
            ident.database, ident.schema, ident.table
        );
        self.db
            .get(&key)
            .await
            .context(metastore_err::UtilSlateDBSnafu)
    }

    #[instrument(name = "SqliteMetastore::table_object_store", level = "debug", skip(self))]
    async fn table_object_store(&self, ident: &TableIdent) -> Result<Option<Arc<dyn ObjectStore>>> {
        if let Some(volume) = self.volume_for_table(ident).await? {
            self.volume_object_store(&volume.ident).await
        } else {
            Ok(None)
        }
    }

    #[instrument(name = "SqliteMetastore::table_exists", level = "debug", skip(self))]
    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        self.get_table(ident).await.map(|table| table.is_some())
    }

    #[instrument(name = "SqliteMetastore::url_for_table", level = "debug", skip(self))]
    async fn url_for_table(&self, ident: &TableIdent) -> Result<String> {
        if let Some(tbl) = self.get_table(ident).await? {
            let database = self.get_database(&ident.database).await?.ok_or_else(|| {
                metastore_err::DatabaseNotFoundSnafu {
                    db: ident.database.clone(),
                }
                .build()
            })?;

            // Table has a custom volume associated
            if let Some(volume_ident) = tbl.volume_ident.as_ref() {
                let volume = self.get_volume(volume_ident).await?.ok_or_else(|| {
                    metastore_err::VolumeNotFoundSnafu {
                        volume: volume_ident.clone(),
                    }
                    .build()
                })?;

                let prefix = volume.prefix();
                // The table has a custom location within the volume
                if let Some(location) = tbl.volume_location.as_ref() {
                    return Ok(format!("{prefix}/{location}"));
                }
                return Ok(format!(
                    "{}/{}/{}/{}",
                    prefix, ident.database, ident.schema, ident.table
                ));
            }

            let volume = self.get_volume(&database.volume).await?.ok_or_else(|| {
                metastore_err::VolumeNotFoundSnafu {
                    volume: database.volume.clone(),
                }
                .build()
            })?;

            let prefix = volume.prefix();

            // The table has a custom location within the volume
            if let Some(location) = tbl.volume_location.as_ref() {
                return Ok(format!("{prefix}/{location}"));
            }

            return Ok(format!(
                "{}/{}/{}/{}",
                prefix, ident.database, ident.schema, ident.table
            ));
        }

        Err(metastore_err::TableObjectStoreNotFoundSnafu {
            table: ident.table.clone(),
            schema: ident.schema.clone(),
            db: ident.database.clone(),
        }
        .build())
    }

    #[instrument(name = "SqliteMetastore::volume_for_table", level = "debug", skip(self))]
    async fn volume_for_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Volume>>> {
        let volume_ident = if let Some(Some(volume_ident)) = self
            .get_table(ident)
            .await?
            .map(|table| table.volume_ident.clone())
        {
            volume_ident
        } else {
            self.get_database(&ident.database)
                .await?
                .ok_or_else(|| {
                    metastore_err::DatabaseNotFoundSnafu {
                        db: ident.database.clone(),
                    }
                    .build()
                })?
                .volume
                .clone()
        };
        self.get_volume(&volume_ident).await
    }
}

fn convert_schema_fields_to_lowercase(schema: &IcebergSchema) -> Result<IcebergSchema> {
    let converted_fields: Vec<StructField> = schema
        .fields()
        .iter()
        .map(|field| {
            StructField::new(
                field.id,
                &field.name.to_lowercase(),
                field.required,
                field.field_type.clone(),
                field.doc.clone(),
            )
        })
        .collect();

    let mut builder = IcebergSchema::builder();
    builder.with_schema_id(*schema.schema_id());

    for field in converted_fields {
        builder.with_struct_field(field);
    }

    builder.build().context(metastore_err::IcebergSpecSnafu)
}

fn convert_add_schema_update_to_lowercase(updates: &mut Vec<IcebergTableUpdate>) -> Result<()> {
    for update in updates {
        if let IcebergTableUpdate::AddSchema {
            schema,
            last_column_id,
        } = update
        {
            let schema = convert_schema_fields_to_lowercase(schema)?;
            *update = IcebergTableUpdate::AddSchema {
                schema,
                last_column_id: *last_column_id,
            }
        }
    }
    Ok(())
}

fn url_encode(input: &str) -> String {
    url::form_urlencoded::byte_serialize(input.as_bytes()).collect()
}
