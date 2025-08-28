use std::{collections::HashMap, sync::Arc};

#[allow(clippy::wildcard_imports)]
use crate::models::*;
use crate::{
    error::{self as metastore_error, Result},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[strum(serialize_all = "lowercase")]
pub enum MetastoreObjectType {
    Volume,
    Database,
    Schema,
    Table,
}

#[async_trait]
pub trait Metastore: std::fmt::Debug + Send + Sync {
    fn iter_volumes(&self) -> VecScanIterator<RwObject<Volume>>;
    async fn create_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>>;
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>>;
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>>;
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()>;
    async fn volume_object_store(&self, name: &VolumeIdent)
    -> Result<Option<Arc<dyn ObjectStore>>>;

    fn iter_databases(&self) -> VecScanIterator<RwObject<Database>>;
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

    fn iter_schemas(&self, database: &DatabaseIdent) -> VecScanIterator<RwObject<Schema>>;
    async fn create_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>>;
    async fn get_schema(&self, ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>>;
    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>>;
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> Result<()>;

    fn iter_tables(&self, schema: &SchemaIdent) -> VecScanIterator<RwObject<Table>>;
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
}

impl std::fmt::Debug for SlateDBMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBMetastore").finish()
    }
}

impl SlateDBMetastore {
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self {
            db,
            object_store_cache: DashMap::new(),
        }
    }

    // Create a new SlateDBMetastore with a new in-memory database
    pub async fn new_in_memory() -> Arc<Self> {
        Arc::new(Self::new(Db::memory().await))
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
        name = "SlateDBMetastore::create_object",
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
            .context(metastore_error::UtilSlateDBSnafu)?
            .is_none()
        {
            let rwobject = RwObject::new(object);
            self.db
                .put(key, &rwobject)
                .await
                .context(metastore_error::UtilSlateDBSnafu)?;
            Ok(rwobject)
        } else {
            Err(metastore_error::ObjectAlreadyExistsSnafu {
                type_name: object_type.to_string(),
                name: key.to_string(),
            }
            .build())
        }
    }

    #[instrument(
        name = "SlateDBMetastore::update_object",
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
            .context(metastore_error::UtilSlateDBSnafu)?
        {
            rwo.update(object);
            self.db
                .put(key, &rwo)
                .await
                .context(metastore_error::UtilSlateDBSnafu)?;
            Ok(rwo)
        } else {
            Err(metastore_error::ObjectNotFoundSnafu {}.build())
        }
    }

    #[instrument(
        name = "SlateDBMetastore::delete_object",
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
}

#[async_trait]
impl Metastore for SlateDBMetastore {
    fn iter_volumes(&self) -> VecScanIterator<RwObject<Volume>> {
        self.iter_objects(KEY_VOLUME.to_string())
    }

    #[instrument(
        name = "Metastore::create_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn create_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        let key = format!("{KEY_VOLUME}/{name}");
        let object_store = volume.get_object_store()?;
        let rwobject = self
            .create_object(&key, MetastoreObjectType::Volume, volume)
            .await
            .map_err(|e| {
                if matches!(e, metastore_error::Error::ObjectAlreadyExists { .. }) {
                    metastore_error::VolumeAlreadyExistsSnafu {
                        volume: name.clone(),
                    }
                    .build()
                } else {
                    e
                }
            })?;
        self.object_store_cache.insert(name.clone(), object_store);
        Ok(rwobject)
    }

    #[instrument(name = "Metastore::get_volume", level = "debug", skip(self), err)]
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>> {
        let key = format!("{KEY_VOLUME}/{name}");
        self.db
            .get(&key)
            .await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    #[instrument(
        name = "Metastore::update_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        let key = format!("{KEY_VOLUME}/{name}");
        let updated_volume = self.update_object(&key, volume.clone()).await?;
        let object_store = updated_volume.get_object_store()?;
        self.object_store_cache
            .alter(name, |_, _store| object_store.clone());
        Ok(updated_volume)
    }

    #[instrument(name = "Metastore::delete_volume", level = "debug", skip(self), err)]
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()> {
        let key = format!("{KEY_VOLUME}/{name}");
        let databases_using = self
            .iter_databases()
            .collect()
            .await
            .context(metastore_error::UtilSlateDBSnafu)?
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
            Err(metastore_error::VolumeInUseSnafu {
                database: databases_using[..].join(", "),
            }
            .build())
        }
    }

    #[instrument(
        name = "Metastore::volume_object_store",
        level = "debug",
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
                metastore_error::VolumeNotFoundSnafu {
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

    #[instrument(name = "Metastore::iter_databases", level = "debug", skip(self))]
    fn iter_databases(&self) -> VecScanIterator<RwObject<Database>> {
        self.iter_objects(KEY_DATABASE.to_string())
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
        self.get_volume(&database.volume).await?.ok_or_else(|| {
            metastore_error::VolumeNotFoundSnafu {
                volume: database.volume.clone(),
            }
            .build()
        })?;
        let key = format!("{KEY_DATABASE}/{name}");
        self.create_object(&key, MetastoreObjectType::Database, database)
            .await
    }

    #[instrument(name = "Metastore::get_database", level = "debug", skip(self), err)]
    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>> {
        let key = format!("{KEY_DATABASE}/{name}");
        self.db
            .get(&key)
            .await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    #[instrument(
        name = "Metastore::update_database",
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

    #[instrument(name = "Metastore::delete_database", level = "debug", skip(self), err)]
    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> Result<()> {
        let schemas = self
            .iter_schemas(name)
            .collect()
            .await
            .context(metastore_error::UtilSlateDBSnafu)?;
        if cascade {
            let futures = schemas
                .iter()
                .map(|schema| self.delete_schema(&schema.ident, cascade))
                .collect::<Vec<_>>();
            futures::future::try_join_all(futures).await?;
        } else if !schemas.is_empty() {
            return Err(metastore_error::DatabaseInUseSnafu {
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
    #[instrument(name = "Metastore::iter_schemas", level = "debug", skip(self))]
    fn iter_schemas(&self, database: &DatabaseIdent) -> VecScanIterator<RwObject<Schema>> {
        //If database is empty, we are iterating over all schemas
        let key = if database.is_empty() {
            KEY_SCHEMA.to_string()
        } else {
            format!("{KEY_SCHEMA}/{database}")
        };
        self.iter_objects(key)
    }

    #[instrument(
        name = "Metastore::create_schema",
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
            Err(metastore_error::DatabaseNotFoundSnafu {
                db: ident.database.clone(),
            }
            .build())
        }
    }

    #[instrument(name = "Metastore::get_schema", level = "debug", skip(self), err)]
    async fn get_schema(&self, ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.db
            .get(&key)
            .await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    #[instrument(
        name = "Metastore::update_schema",
        level = "debug",
        skip(self, schema),
        err
    )]
    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.update_object(&key, schema).await
    }

    #[instrument(name = "Metastore::delete_schema", level = "debug", skip(self), err)]
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> Result<()> {
        let tables = self
            .iter_tables(ident)
            .collect()
            .await
            .context(metastore_error::UtilSlateDBSnafu)?;
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

    #[instrument(name = "Metastore::iter_tables", level = "debug", skip(self))]
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
    #[instrument(name = "Metastore::create_table", level = "debug", skip(self), err)]
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
                let volume = self.create_volume(&volume_ident, volume).await?;
                if table.volume_ident.is_none() {
                    table.volume_ident = Some(volume_ident);
                }

                table.location.as_ref().map_or_else(
                    || volume.prefix(),
                    |volume_location| format!("{}/{volume_location}", volume.prefix()),
                )
            } else {
                let database = self.get_database(&ident.database).await?.ok_or_else(|| {
                    metastore_error::DatabaseNotFoundSnafu {
                        db: ident.database.clone(),
                    }
                    .build()
                })?;
                let volume = self.get_volume(&database.volume).await?.ok_or_else(|| {
                    metastore_error::VolumeNotFoundSnafu {
                        volume: database.volume.clone(),
                    }
                    .build()
                })?;

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
                .context(metastore_error::TableMetadataBuilderSnafu)?;

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
                metastore_error::TableObjectStoreNotFoundSnafu {
                    table: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
            let data = Bytes::from(
                serde_json::to_vec(&table_metadata).context(metastore_error::SerdeSnafu)?,
            );

            let url = url::Url::parse(&table.metadata_location)
                .context(metastore_error::UrlParseSnafu)?;
            let path = Path::from(url.path());
            object_store
                .put(&path, PutPayload::from(data))
                .await
                .context(metastore_error::ObjectStoreSnafu)?;
            Ok(rwo_table)
        } else {
            Err(metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .build())
        }
    }

    #[instrument(
        name = "Metastore::update_table",
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
                metastore_error::TableNotFoundSnafu {
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
            .context(metastore_error::IcebergSnafu)?;

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
            metastore_error::DatabaseNotFoundSnafu {
                db: ident.database.clone(),
            }
            .build()
        })?;
        let volume = self.get_volume(&db.volume).await?.ok_or_else(|| {
            metastore_error::VolumeNotFoundSnafu {
                volume: db.volume.clone(),
            }
            .build()
        })?;

        let object_store = volume.get_object_store()?;
        let data =
            Bytes::from(serde_json::to_vec(&table.metadata).context(metastore_error::SerdeSnafu)?);

        let url = url::Url::parse(&metadata_location).context(metastore_error::UrlParseSnafu)?;
        let path = Path::from(url.path());

        object_store
            .put(&path, PutPayload::from(data))
            .await
            .context(metastore_error::ObjectStoreSnafu)?;

        Ok(rw_table)
    }

    #[instrument(name = "Metastore::delete_table", level = "debug", skip(self), err)]
    async fn delete_table(&self, ident: &TableIdent, cascade: bool) -> Result<()> {
        if let Some(table) = self.get_table(ident).await? {
            if cascade {
                let object_store = self.table_object_store(ident).await?.ok_or_else(|| {
                    metastore_error::TableObjectStoreNotFoundSnafu {
                        table: ident.table.clone(),
                        schema: ident.schema.clone(),
                        db: ident.database.clone(),
                    }
                    .build()
                })?;
                let url = url::Url::parse(&self.url_for_table(ident).await?)
                    .context(metastore_error::UrlParseSnafu)?;
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
                    .context(metastore_error::ObjectStoreSnafu)?;
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
            Err(metastore_error::TableNotFoundSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .build())
        }
    }

    #[instrument(name = "Metastore::get_table", level = "debug", skip(self))]
    async fn get_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Table>>> {
        let key = format!(
            "{KEY_TABLE}/{}/{}/{}",
            ident.database, ident.schema, ident.table
        );
        self.db
            .get(&key)
            .await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    #[instrument(name = "Metastore::table_object_store", level = "debug", skip(self))]
    async fn table_object_store(&self, ident: &TableIdent) -> Result<Option<Arc<dyn ObjectStore>>> {
        if let Some(volume) = self.volume_for_table(ident).await? {
            self.volume_object_store(&volume.ident).await
        } else {
            Ok(None)
        }
    }

    #[instrument(name = "Metastore::table_exists", level = "debug", skip(self))]
    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        self.get_table(ident).await.map(|table| table.is_some())
    }

    #[instrument(name = "Metastore::url_for_table", level = "debug", skip(self))]
    async fn url_for_table(&self, ident: &TableIdent) -> Result<String> {
        if let Some(tbl) = self.get_table(ident).await? {
            let database = self.get_database(&ident.database).await?.ok_or_else(|| {
                metastore_error::DatabaseNotFoundSnafu {
                    db: ident.database.clone(),
                }
                .build()
            })?;

            // Table has a custom volume associated
            if let Some(volume_ident) = tbl.volume_ident.as_ref() {
                let volume = self.get_volume(volume_ident).await?.ok_or_else(|| {
                    metastore_error::VolumeNotFoundSnafu {
                        volume: volume_ident.clone(),
                    }
                    .build()
                })?;

                let prefix = volume.prefix();
                // The location of the table within the custom volume
                let location = tbl
                    .volume_location
                    .clone()
                    .unwrap_or_else(|| "/".to_string());
                return Ok(format!("{prefix}/{location}"));
            }

            let volume = self.get_volume(&database.volume).await?.ok_or_else(|| {
                metastore_error::VolumeNotFoundSnafu {
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

        return Err(metastore_error::TableObjectStoreNotFoundSnafu {
            table: ident.table.clone(),
            schema: ident.schema.clone(),
            db: ident.database.clone(),
        }
        .build());
    }

    #[instrument(name = "Metastore::volume_for_table", level = "debug", skip(self))]
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
                    metastore_error::DatabaseNotFoundSnafu {
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

    builder.build().context(metastore_error::IcebergSpecSnafu)
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use iceberg_rust_spec::{
        schema::Schema as IcebergSchema,
        types::{PrimitiveType, StructField, Type},
    };
    use slatedb::Db as SlateDb;
    use std::result::Result;
    use std::sync::Arc;

    fn insta_filters() -> Vec<(&'static str, &'static str)> {
        vec![
            (r"created_at[^,]*", "created_at: \"TIMESTAMP\""),
            (r"updated_at[^,]*", "updated_at: \"TIMESTAMP\""),
            (r"last_modified[^,]*", "last_modified: \"TIMESTAMP\""),
            (r"size[^,]*", "size: \"INTEGER\""),
            (r"last_updated_ms[^,]*", "last_update_ms: \"INTEGER\""),
            (
                r"[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
                "UUID",
            ),
            (r"lookup: \{[^}]*\}", "lookup: {LOOKUPS}"),
            (r"properties: \{[^}]*\}", "properties: {PROPERTIES}"),
            (r"at .*.rs:\d+:\d+", "at file:line:col"), // remove Error location
        ]
    }

    async fn get_metastore() -> SlateDBMetastore {
        let object_store = object_store::memory::InMemory::new();
        let sdb = SlateDb::open(Path::from("/"), Arc::new(object_store))
            .await
            .expect("Failed to open db");
        let db = Db::new(Arc::new(sdb));
        SlateDBMetastore::new(db)
    }

    #[tokio::test]
    async fn test_create_volumes() {
        let ms = get_metastore().await;

        let volume = Volume::new("test".to_owned(), VolumeType::Memory);
        ms.create_volume(&"test".to_string(), volume)
            .await
            .expect("create volume failed");
        let all_volumes = ms
            .iter_volumes()
            .collect()
            .await
            .expect("list volumes failed");

        let test_volume = ms
            .db()
            .get::<serde_json::Value>(&format!("{KEY_VOLUME}/test"))
            .await
            .expect("get test volume failed");

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((test_volume, all_volumes));
        });
    }

    #[tokio::test]
    async fn test_create_s3table_volume() {
        let ms = get_metastore().await;

        let s3table_volume = VolumeType::S3Tables(S3TablesVolume {
            arn: "arn:aws:s3tables:us-east-1:111122223333:bucket/my-table-bucket".to_string(),
            endpoint: Some("https://my-bucket-name.s3.us-east-1.amazonaws.com/".to_string()),
            credentials: AwsCredentials::AccessKey(AwsAccessKeyCredentials {
                aws_access_key_id: "kPYGGu34jF685erC7gst".to_string(),
                aws_secret_access_key: "Q2ClWJgwIZLcX4IE2zO2GBl8qXz7g4knqwLwUpWL".to_string(),
            }),
        });
        let volume = Volume::new("s3tables".to_string(), s3table_volume);
        ms.create_volume(&volume.ident.clone(), volume.clone())
            .await
            .expect("create s3table volume failed");

        let created_volume = ms
            .get_volume(&volume.ident)
            .await
            .expect("get s3table volume failed");
        let created_volume = created_volume.expect("No volume in Option").data;

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((volume, created_volume));
        });
    }

    #[tokio::test]
    async fn test_duplicate_volume() {
        let ms = get_metastore().await;

        let volume = Volume::new("test".to_owned(), VolumeType::Memory);
        ms.create_volume(&"test".to_owned(), volume)
            .await
            .expect("create volume failed");

        let volume2 = Volume::new("test".to_owned(), VolumeType::Memory);
        let result = ms.create_volume(&"test".to_owned(), volume2).await;
        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!(result);
        });
    }

    #[tokio::test]
    async fn test_delete_volume() {
        let ms = get_metastore().await;

        let volume = Volume::new("test".to_owned(), VolumeType::Memory);
        ms.create_volume(&"test".to_string(), volume)
            .await
            .expect("create volume failed");
        let all_volumes = ms
            .iter_volumes()
            .collect()
            .await
            .expect("list volumes failed");
        let get_volume = ms
            .get_volume(&"test".to_owned())
            .await
            .expect("get volume failed");
        ms.delete_volume(&"test".to_string(), false)
            .await
            .expect("delete volume failed");
        let all_volumes_after = ms
            .iter_volumes()
            .collect()
            .await
            .expect("list volumes failed");

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((all_volumes, get_volume, all_volumes_after ));
        });
    }

    #[tokio::test]
    async fn test_update_volume() {
        let ms = get_metastore().await;

        let volume = Volume::new("test".to_owned(), VolumeType::Memory);
        let rwo1 = ms
            .create_volume(&"test".to_owned(), volume)
            .await
            .expect("create volume failed");
        let volume = Volume::new(
            "test".to_owned(),
            VolumeType::File(FileVolume {
                path: "/tmp".to_owned(),
            }),
        );
        let rwo2 = ms
            .update_volume(&"test".to_owned(), volume)
            .await
            .expect("update volume failed");
        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((rwo1, rwo2));
        });
    }

    #[tokio::test]
    async fn test_create_database() {
        let ms = get_metastore().await;
        let mut database = Database {
            ident: "testdb".to_owned(),
            volume: "testv1".to_owned(),
            properties: None,
        };
        let no_volume_result = ms
            .create_database(&"testdb".to_owned(), database.clone())
            .await;

        let volume = Volume::new("test".to_owned(), VolumeType::Memory);
        let volume2 = Volume::new(
            "test2".to_owned(),
            VolumeType::File(FileVolume {
                path: "/tmp".to_owned(),
            }),
        );
        ms.create_volume(&"testv1".to_owned(), volume)
            .await
            .expect("create volume failed");
        ms.create_volume(&"testv2".to_owned(), volume2)
            .await
            .expect("create volume failed");
        ms.create_database(&"testdb".to_owned(), database.clone())
            .await
            .expect("create database failed");
        let all_databases = ms
            .iter_databases()
            .collect()
            .await
            .expect("list databases failed");

        database.volume = "testv2".to_owned();
        ms.update_database(&"testdb".to_owned(), database)
            .await
            .expect("update database failed");
        let fetched_db = ms
            .get_database(&"testdb".to_owned())
            .await
            .expect("get database failed");

        ms.delete_database(&"testdb".to_string(), false)
            .await
            .expect("delete database failed");
        let all_dbs_after = ms
            .iter_databases()
            .collect()
            .await
            .expect("list databases failed");

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((no_volume_result, all_databases, fetched_db, all_dbs_after));
        });
    }

    #[tokio::test]
    async fn test_schemas() {
        let ms = get_metastore().await;
        let schema = Schema {
            ident: SchemaIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
            },
            properties: None,
        };

        let no_db_result = ms
            .create_schema(&schema.ident.clone(), schema.clone())
            .await;

        let volume = Volume::new("test".to_owned(), VolumeType::Memory);
        ms.create_volume(&"testv1".to_owned(), volume)
            .await
            .expect("create volume failed");
        ms.create_database(
            &"testdb".to_owned(),
            Database {
                ident: "testdb".to_owned(),
                volume: "testv1".to_owned(),
                properties: None,
            },
        )
        .await
        .expect("create database failed");
        let schema_create = ms
            .create_schema(&schema.ident.clone(), schema.clone())
            .await
            .expect("create schema failed");

        let schema_list = ms
            .iter_schemas(&schema.ident.database)
            .collect()
            .await
            .expect("list schemas failed");
        let schema_get = ms
            .get_schema(&schema.ident)
            .await
            .expect("get schema failed");
        ms.delete_schema(&schema.ident, false)
            .await
            .expect("delete schema failed");
        let schema_list_after = ms
            .iter_schemas(&schema.ident.database)
            .collect()
            .await
            .expect("list schemas failed");

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((no_db_result, schema_create, schema_list, schema_get, schema_list_after));
        });
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_tables() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let sdb = SlateDb::open(Path::from("/"), object_store.clone())
            .await
            .expect("Failed to open db");
        let db = Db::new(Arc::new(sdb));
        let ms = SlateDBMetastore::new(db);

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_struct_field(StructField::new(
                0,
                "id",
                true,
                Type::Primitive(PrimitiveType::Int),
                None,
            ))
            .with_struct_field(StructField::new(
                1,
                "name",
                true,
                Type::Primitive(PrimitiveType::String),
                None,
            ))
            .build()
            .expect("schema build failed");

        let table = TableCreateRequest {
            ident: TableIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
                table: "testtable".to_owned(),
            },
            format: None,
            properties: None,
            location: None,
            schema,
            partition_spec: None,
            sort_order: None,
            stage_create: None,
            volume_ident: None,
            is_temporary: None,
        };

        let no_schema_result = ms.create_table(&table.ident.clone(), table.clone()).await;

        let volume = Volume::new("testv1".to_owned(), VolumeType::Memory);
        ms.create_volume(&"testv1".to_owned(), volume)
            .await
            .expect("create volume failed");
        ms.create_database(
            &"testdb".to_owned(),
            Database {
                ident: "testdb".to_owned(),
                volume: "testv1".to_owned(),
                properties: None,
            },
        )
        .await
        .expect("create database failed");
        ms.create_schema(
            &SchemaIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
            },
            Schema {
                ident: SchemaIdent {
                    database: "testdb".to_owned(),
                    schema: "testschema".to_owned(),
                },
                properties: None,
            },
        )
        .await
        .expect("create schema failed");
        let table_create = ms
            .create_table(&table.ident.clone(), table.clone())
            .await
            .expect("create table failed");
        let vol_object_store = ms
            .volume_object_store(&"testv1".to_owned())
            .await
            .expect("get volume object store failed")
            .expect("Object store not found");
        let paths: Result<Vec<_>, ()> = vol_object_store
            .list(None)
            .then(|c| async move { Ok::<_, ()>(c) })
            .collect::<Vec<Result<_, _>>>()
            .await
            .into_iter()
            .collect();

        let table_list = ms
            .iter_tables(&table.ident.clone().into())
            .collect()
            .await
            .expect("list tables failed");
        let table_get = ms.get_table(&table.ident).await.expect("get table failed");
        ms.delete_table(&table.ident, false)
            .await
            .expect("delete table failed");
        let table_list_after = ms
            .iter_tables(&table.ident.into())
            .collect()
            .await
            .expect("list tables failed");

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!(
                (
                    no_schema_result,
                    table_create,
                    paths,
                    table_list,
                    table_get,
                    table_list_after
                )
            );
        });
    }

    #[tokio::test]
    async fn test_temporary_tables() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let sdb = SlateDb::open(Path::from("/"), object_store.clone())
            .await
            .expect("Failed to open db");
        let db = Db::new(Arc::new(sdb));
        let ms = SlateDBMetastore::new(db);

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_struct_field(StructField::new(
                0,
                "id",
                true,
                Type::Primitive(PrimitiveType::Int),
                None,
            ))
            .with_struct_field(StructField::new(
                1,
                "name",
                true,
                Type::Primitive(PrimitiveType::String),
                None,
            ))
            .build()
            .expect("schema build failed");

        let table = TableCreateRequest {
            ident: TableIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
                table: "testtable".to_owned(),
            },
            format: None,
            properties: None,
            location: None,
            schema,
            partition_spec: None,
            sort_order: None,
            stage_create: None,
            volume_ident: None,
            is_temporary: Some(true),
        };

        let volume = Volume::new("testv1".to_owned(), VolumeType::Memory);
        ms.create_volume(&"testv1".to_owned(), volume)
            .await
            .expect("create volume failed");
        ms.create_database(
            &"testdb".to_owned(),
            Database {
                ident: "testdb".to_owned(),
                volume: "testv1".to_owned(),
                properties: None,
            },
        )
        .await
        .expect("create database failed");
        ms.create_schema(
            &SchemaIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
            },
            Schema {
                ident: SchemaIdent {
                    database: "testdb".to_owned(),
                    schema: "testschema".to_owned(),
                },
                properties: None,
            },
        )
        .await
        .expect("create schema failed");
        let create_table = ms
            .create_table(&table.ident.clone(), table.clone())
            .await
            .expect("create table failed");
        let vol_object_store = ms
            .table_object_store(&create_table.ident)
            .await
            .expect("get table object store failed")
            .expect("Object store not found");

        let paths: Result<Vec<_>, ()> = vol_object_store
            .list(None)
            .then(|c| async move { Ok::<_, ()>(c) })
            .collect::<Vec<Result<_, _>>>()
            .await
            .into_iter()
            .collect();

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((create_table.volume_ident.as_ref(), paths));
        });
    }

    // TODO: Add custom table location tests
}
