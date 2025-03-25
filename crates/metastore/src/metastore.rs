// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{collections::HashMap, sync::Arc};

use crate::error::{self as metastore_error, MetastoreResult};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use iceberg_rust::catalog::commit::apply_table_updates;
use iceberg_rust_spec::table_metadata::{FormatVersion, TableMetadataBuilder};
use icebucket_utils::Db;
use object_store::{path::Path, ObjectStore, PutPayload};
use serde::de::DeserializeOwned;
use snafu::ResultExt;
use uuid::Uuid;

#[allow(clippy::wildcard_imports)]
use crate::models::*;

#[async_trait]
pub trait Metastore: std::fmt::Debug + Send + Sync {
    async fn list_volumes(&self) -> MetastoreResult<Vec<RwObject<IceBucketVolume>>>;
    async fn create_volume(
        &self,
        name: &IceBucketVolumeIdent,
        volume: IceBucketVolume,
    ) -> MetastoreResult<RwObject<IceBucketVolume>>;
    async fn get_volume(
        &self,
        name: &IceBucketVolumeIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketVolume>>>;
    async fn update_volume(
        &self,
        name: &IceBucketVolumeIdent,
        volume: IceBucketVolume,
    ) -> MetastoreResult<RwObject<IceBucketVolume>>;
    async fn delete_volume(
        &self,
        name: &IceBucketVolumeIdent,
        cascade: bool,
    ) -> MetastoreResult<()>;
    async fn volume_object_store(
        &self,
        name: &IceBucketVolumeIdent,
    ) -> MetastoreResult<Option<Arc<dyn ObjectStore>>>;

    async fn list_databases(&self) -> MetastoreResult<Vec<RwObject<IceBucketDatabase>>>;
    async fn create_database(
        &self,
        name: &IceBucketDatabaseIdent,
        database: IceBucketDatabase,
    ) -> MetastoreResult<RwObject<IceBucketDatabase>>;
    async fn get_database(
        &self,
        name: &IceBucketDatabaseIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketDatabase>>>;
    async fn update_database(
        &self,
        name: &IceBucketDatabaseIdent,
        database: IceBucketDatabase,
    ) -> MetastoreResult<RwObject<IceBucketDatabase>>;
    async fn delete_database(
        &self,
        name: &IceBucketDatabaseIdent,
        cascade: bool,
    ) -> MetastoreResult<()>;

    async fn list_schemas(
        &self,
        database: &IceBucketDatabaseIdent,
    ) -> MetastoreResult<Vec<RwObject<IceBucketSchema>>>;
    async fn create_schema(
        &self,
        ident: &IceBucketSchemaIdent,
        schema: IceBucketSchema,
    ) -> MetastoreResult<RwObject<IceBucketSchema>>;
    async fn get_schema(
        &self,
        ident: &IceBucketSchemaIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketSchema>>>;
    async fn update_schema(
        &self,
        ident: &IceBucketSchemaIdent,
        schema: IceBucketSchema,
    ) -> MetastoreResult<RwObject<IceBucketSchema>>;
    async fn delete_schema(
        &self,
        ident: &IceBucketSchemaIdent,
        cascade: bool,
    ) -> MetastoreResult<()>;

    async fn list_tables(
        &self,
        schema: &IceBucketSchemaIdent,
    ) -> MetastoreResult<Vec<RwObject<IceBucketTable>>>;
    async fn create_table(
        &self,
        ident: &IceBucketTableIdent,
        table: IceBucketTableCreateRequest,
    ) -> MetastoreResult<RwObject<IceBucketTable>>;
    async fn get_table(
        &self,
        ident: &IceBucketTableIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketTable>>>;
    async fn update_table(
        &self,
        ident: &IceBucketTableIdent,
        update: IceBucketTableUpdate,
    ) -> MetastoreResult<RwObject<IceBucketTable>>;
    async fn delete_table(&self, ident: &IceBucketTableIdent, cascade: bool)
        -> MetastoreResult<()>;
    async fn table_object_store(
        &self,
        ident: &IceBucketTableIdent,
    ) -> MetastoreResult<Option<Arc<dyn ObjectStore>>>;

    async fn table_exists(&self, ident: &IceBucketTableIdent) -> MetastoreResult<bool>;
    async fn url_for_table(&self, ident: &IceBucketTableIdent) -> MetastoreResult<String>;
    async fn volume_for_table(
        &self,
        ident: &IceBucketTableIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketVolume>>>;
}

///
/// vol -> List of volumes
/// vol/<name> -> `IceBucketVolume`
/// db -> List of databases
/// db/<name> -> `IceBucketDatabase`
/// sch/<db> -> List of schemas for <db>
/// sch/<db>/<name> -> `IceBucketSchema`
/// tbl/<db>/<schema> -> List of tables for <schema> in <db>
/// tbl/<db>/<schema>/<table> -> `IceBucketTable`
///
const KEY_VOLUME: &str = "vol";
const KEY_DATABASE: &str = "db";
const KEY_SCHEMA: &str = "sch";
const KEY_TABLE: &str = "tbl";

pub struct SlateDBMetastore {
    db: Db,
    object_store_cache: DashMap<IceBucketVolumeIdent, Arc<dyn ObjectStore>>,
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

    async fn list_objects<T>(&self, list_key: &str) -> MetastoreResult<Vec<RwObject<T>>>
    where
        T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync,
    {
        let entities = self
            .db
            .list_objects(list_key)
            .await
            .context(metastore_error::UtilSlateDBSnafu)?;
        Ok(entities)
    }

    async fn create_object<T>(
        &self,
        key: &str,
        object_type: &str,
        object: T,
    ) -> MetastoreResult<RwObject<T>>
    where
        T: serde::Serialize + DeserializeOwned + Eq + PartialEq + Send + Sync,
    {
        if self
            .db
            .get::<T>(key)
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
            Err(metastore_error::MetastoreError::ObjectAlreadyExists {
                type_name: object_type.to_owned(),
                name: key.to_string(),
            })
        }
    }

    async fn update_object<T>(&self, key: &str, object: T) -> MetastoreResult<RwObject<T>>
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
            Err(metastore_error::MetastoreError::ObjectNotFound)
        }
    }

    async fn delete_object(&self, key: &str) -> MetastoreResult<()> {
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
    async fn list_volumes(&self) -> MetastoreResult<Vec<RwObject<IceBucketVolume>>> {
        self.list_objects(KEY_VOLUME).await
    }

    async fn create_volume(
        &self,
        name: &IceBucketVolumeIdent,
        volume: IceBucketVolume,
    ) -> MetastoreResult<RwObject<IceBucketVolume>> {
        let key = format!("{KEY_VOLUME}/{name}");
        let object_store = volume.get_object_store()?;
        let rwobject = self
            .create_object(&key, "volume", volume)
            .await
            .map_err(|e| {
                if matches!(
                    e,
                    metastore_error::MetastoreError::ObjectAlreadyExists { .. }
                ) {
                    metastore_error::MetastoreError::VolumeAlreadyExists {
                        volume: name.clone(),
                    }
                } else {
                    e
                }
            })?;
        self.object_store_cache.insert(name.clone(), object_store);
        Ok(rwobject)
    }

    async fn get_volume(
        &self,
        name: &IceBucketVolumeIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketVolume>>> {
        let key = format!("{KEY_VOLUME}/{name}");
        self.db
            .get(&key)
            .await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    async fn update_volume(
        &self,
        name: &IceBucketVolumeIdent,
        volume: IceBucketVolume,
    ) -> MetastoreResult<RwObject<IceBucketVolume>> {
        let key = format!("{KEY_VOLUME}/{name}");
        self.update_object(&key, volume).await
    }

    async fn delete_volume(
        &self,
        name: &IceBucketVolumeIdent,
        cascade: bool,
    ) -> MetastoreResult<()> {
        let key = format!("{KEY_VOLUME}/{name}");
        let databases_using = self
            .list_databases()
            .await?
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
            Err(metastore_error::MetastoreError::VolumeInUse {
                database: databases_using[..].join(", "),
            })
        }
    }

    async fn volume_object_store(
        &self,
        name: &IceBucketVolumeIdent,
    ) -> MetastoreResult<Option<Arc<dyn ObjectStore>>> {
        if let Some(store) = self.object_store_cache.get(name) {
            Ok(Some(store.clone()))
        } else {
            let volume = self.get_volume(name).await?.ok_or(
                metastore_error::MetastoreError::VolumeNotFound {
                    volume: name.clone(),
                },
            )?;
            let object_store = volume.get_object_store()?;
            self.object_store_cache
                .insert(name.clone(), object_store.clone());
            Ok(Some(object_store))
        }
    }

    async fn list_databases(&self) -> MetastoreResult<Vec<RwObject<IceBucketDatabase>>> {
        self.list_objects(KEY_DATABASE).await
    }

    async fn create_database(
        &self,
        name: &IceBucketDatabaseIdent,
        database: IceBucketDatabase,
    ) -> MetastoreResult<RwObject<IceBucketDatabase>> {
        self.get_volume(&database.volume).await?.ok_or(
            metastore_error::MetastoreError::VolumeNotFound {
                volume: database.volume.clone(),
            },
        )?;
        let key = format!("{KEY_DATABASE}/{name}");
        self.create_object(&key, "database", database).await
    }

    async fn get_database(
        &self,
        name: &IceBucketDatabaseIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketDatabase>>> {
        let key = format!("{KEY_DATABASE}/{name}");
        self.db
            .get(&key)
            .await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    async fn update_database(
        &self,
        name: &IceBucketDatabaseIdent,
        database: IceBucketDatabase,
    ) -> MetastoreResult<RwObject<IceBucketDatabase>> {
        let key = format!("{KEY_DATABASE}/{name}");
        self.update_object(&key, database).await
    }

    async fn delete_database(
        &self,
        name: &IceBucketDatabaseIdent,
        cascade: bool,
    ) -> MetastoreResult<()> {
        let schemas = self.list_schemas(name).await?;
        if cascade {
            let futures = schemas
                .iter()
                .map(|schema| self.delete_schema(&schema.ident, cascade))
                .collect::<Vec<_>>();
            futures::future::try_join_all(futures).await?;
        }
        let key = format!("{KEY_DATABASE}/{name}");
        self.delete_object(&key).await
    }

    async fn list_schemas(
        &self,
        database: &IceBucketDatabaseIdent,
    ) -> MetastoreResult<Vec<RwObject<IceBucketSchema>>> {
        let key = format!("{KEY_SCHEMA}/{database}");
        self.list_objects(&key).await
    }

    async fn create_schema(
        &self,
        ident: &IceBucketSchemaIdent,
        schema: IceBucketSchema,
    ) -> MetastoreResult<RwObject<IceBucketSchema>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        if self.get_database(&ident.database).await?.is_some() {
            self.create_object(&key, "schema", schema).await
        } else {
            Err(metastore_error::MetastoreError::DatabaseNotFound {
                db: ident.database.clone(),
            })
        }
    }

    async fn get_schema(
        &self,
        ident: &IceBucketSchemaIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketSchema>>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.db
            .get(&key)
            .await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    async fn update_schema(
        &self,
        ident: &IceBucketSchemaIdent,
        schema: IceBucketSchema,
    ) -> MetastoreResult<RwObject<IceBucketSchema>> {
        let key = format!("{KEY_SCHEMA}/{}/{}", ident.database, ident.schema);
        self.update_object(&key, schema).await
    }

    async fn delete_schema(
        &self,
        ident: &IceBucketSchemaIdent,
        cascade: bool,
    ) -> MetastoreResult<()> {
        let tables = self.list_tables(ident).await?;

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

    async fn list_tables(
        &self,
        schema: &IceBucketSchemaIdent,
    ) -> MetastoreResult<Vec<RwObject<IceBucketTable>>> {
        let key = format!("{KEY_TABLE}/{}/{}", schema.database, schema.schema);
        self.list_objects(&key).await
    }

    #[allow(clippy::too_many_lines)]
    async fn create_table(
        &self,
        ident: &IceBucketTableIdent,
        mut table: IceBucketTableCreateRequest,
    ) -> MetastoreResult<RwObject<IceBucketTable>> {
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
                let volume = IceBucketVolume {
                    ident: volume_ident.clone(),
                    volume: IceBucketVolumeType::Memory,
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
                let database = self.get_database(&ident.database).await?.ok_or(
                    metastore_error::MetastoreError::DatabaseNotFound {
                        db: ident.database.clone(),
                    },
                )?;
                let volume = self.get_volume(&database.volume).await?.ok_or(
                    metastore_error::MetastoreError::VolumeNotFound {
                        volume: database.volume.clone(),
                    },
                )?;

                let prefix = volume.prefix();
                format!(
                    "{prefix}/{}/{}/{}",
                    ident.database, ident.schema, ident.table
                )
            };

            let metadata_part = format!("metadata/{}", Self::generate_metadata_filename());

            let mut table_metadata = TableMetadataBuilder::default();

            table_metadata
                .current_schema_id(*table.schema.schema_id())
                .with_schema((0, table.schema))
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

            let table_format = table.format.unwrap_or(IceBucketTableFormat::Iceberg);

            let table_metadata = table_metadata
                .build()
                .context(metastore_error::TableMetadataBuilderSnafu)?;

            let mut table_properties = table.properties.unwrap_or_default().clone();
            Self::update_properties_timestamps(&mut table_properties);

            let table = IceBucketTable {
                ident: ident.clone(),
                metadata: table_metadata.clone(),
                metadata_location: format!("{table_location}/{metadata_part}"),
                properties: table_properties,
                volume_ident: table.volume_ident,
                volume_location: table.location,
                is_temporary: table.is_temporary.unwrap_or_default(),
                format: table_format,
            };
            let rwo_table = self.create_object(&key, "table", table.clone()).await?;

            let object_store = self.table_object_store(ident).await?.ok_or(
                metastore_error::MetastoreError::TableObjectStoreNotFound {
                    table: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                },
            )?;
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
            Err(metastore_error::MetastoreError::SchemaNotFound {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            })
        }
    }

    async fn update_table(
        &self,
        ident: &IceBucketTableIdent,
        update: IceBucketTableUpdate,
    ) -> MetastoreResult<RwObject<IceBucketTable>> {
        let mut table = self
            .get_table(ident)
            .await?
            .ok_or(metastore_error::MetastoreError::TableNotFound {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            })?
            .data;

        update
            .requirements
            .into_iter()
            .map(TableRequirementExt::new)
            .try_for_each(|req| req.assert(&table.metadata, true))?;

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

        let db = self.get_database(&ident.database).await?.ok_or(
            metastore_error::MetastoreError::DatabaseNotFound {
                db: ident.database.clone(),
            },
        )?;
        let volume = self.get_volume(&db.volume).await?.ok_or(
            metastore_error::MetastoreError::VolumeNotFound {
                volume: db.volume.clone(),
            },
        )?;

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

    async fn delete_table(
        &self,
        ident: &IceBucketTableIdent,
        cascade: bool,
    ) -> MetastoreResult<()> {
        if let Some(table) = self.get_table(ident).await? {
            if cascade {
                let object_store = self.table_object_store(ident).await?.ok_or(
                    metastore_error::MetastoreError::TableObjectStoreNotFound {
                        table: ident.table.clone(),
                        schema: ident.schema.clone(),
                        db: ident.database.clone(),
                    },
                )?;
                let metadata_path = Path::from(self.url_for_table(ident).await?);
                object_store
                    .delete(&metadata_path)
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
            Err(metastore_error::MetastoreError::TableNotFound {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            })
        }
    }

    async fn get_table(
        &self,
        ident: &IceBucketTableIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketTable>>> {
        let key = format!(
            "{KEY_TABLE}/{}/{}/{}",
            ident.database, ident.schema, ident.table
        );
        self.db
            .get(&key)
            .await
            .context(metastore_error::UtilSlateDBSnafu)
    }

    async fn table_object_store(
        &self,
        ident: &IceBucketTableIdent,
    ) -> MetastoreResult<Option<Arc<dyn ObjectStore>>> {
        if let Some(volume) = self.volume_for_table(ident).await? {
            self.volume_object_store(&volume.ident).await
        } else {
            Ok(None)
        }
    }

    async fn table_exists(&self, ident: &IceBucketTableIdent) -> MetastoreResult<bool> {
        self.get_table(ident).await.map(|table| table.is_some())
    }

    async fn url_for_table(&self, ident: &IceBucketTableIdent) -> MetastoreResult<String> {
        if let Some(tbl) = self.get_table(ident).await? {
            let database = self.get_database(&ident.database).await?.ok_or(
                metastore_error::MetastoreError::DatabaseNotFound {
                    db: ident.database.clone(),
                },
            )?;

            // Table has a custom volume associated
            if let Some(volume_ident) = tbl.volume_ident.as_ref() {
                let volume = self.get_volume(volume_ident).await?.ok_or(
                    metastore_error::MetastoreError::VolumeNotFound {
                        volume: volume_ident.clone(),
                    },
                )?;

                let prefix = volume.prefix();
                // The location of the table within the custom volume
                let location = tbl
                    .volume_location
                    .clone()
                    .unwrap_or_else(|| "/".to_string());
                return Ok(format!("{prefix}/{location}"));
            }

            let volume = self.get_volume(&database.volume).await?.ok_or(
                metastore_error::MetastoreError::VolumeNotFound {
                    volume: database.volume.clone(),
                },
            )?;

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

        return Err(metastore_error::MetastoreError::TableObjectStoreNotFound {
            table: ident.table.clone(),
            schema: ident.schema.clone(),
            db: ident.database.clone(),
        });
    }

    async fn volume_for_table(
        &self,
        ident: &IceBucketTableIdent,
    ) -> MetastoreResult<Option<RwObject<IceBucketVolume>>> {
        let volume_ident = if let Some(Some(volume_ident)) = self
            .get_table(ident)
            .await?
            .map(|table| table.volume_ident.clone())
        {
            volume_ident
        } else {
            self.get_database(&ident.database)
                .await?
                .ok_or(metastore_error::MetastoreError::DatabaseNotFound {
                    db: ident.database.clone(),
                })?
                .volume
                .clone()
        };
        self.get_volume(&volume_ident).await
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use iceberg_rust_spec::{
        schema::Schema,
        types::{PrimitiveType, StructField, StructType, Type},
    };
    use slatedb::db::Db as SlateDb;
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

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume(&"test".to_string(), volume)
            .await
            .expect("create volume failed");
        let all_volumes = ms.list_volumes().await.expect("list volumes failed");

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
    async fn test_duplicate_volume() {
        let ms = get_metastore().await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume(&"test".to_owned(), volume)
            .await
            .expect("create volume failed");

        let volume2 = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
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

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume(&"test".to_string(), volume)
            .await
            .expect("create volume failed");
        let all_volumes = ms.list_volumes().await.expect("list volumes failed");
        let get_volume = ms
            .get_volume(&"test".to_owned())
            .await
            .expect("get volume failed");
        ms.delete_volume(&"test".to_string(), false)
            .await
            .expect("delete volume failed");
        let all_volumes_after = ms.list_volumes().await.expect("list volumes failed");

        insta::with_settings!({
            filters => insta_filters(),
        }, {
            insta::assert_debug_snapshot!((all_volumes, get_volume, all_volumes_after ));
        });
    }

    #[tokio::test]
    async fn test_update_volume() {
        let ms = get_metastore().await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        let rwo1 = ms
            .create_volume(&"test".to_owned(), volume)
            .await
            .expect("create volume failed");
        let volume = IceBucketVolume::new(
            "test".to_owned(),
            IceBucketVolumeType::File(IceBucketFileVolume {
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
        let mut database = IceBucketDatabase {
            ident: "testdb".to_owned(),
            volume: "testv1".to_owned(),
            properties: None,
        };
        let no_volume_result = ms
            .create_database(&"testdb".to_owned(), database.clone())
            .await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        let volume2 = IceBucketVolume::new(
            "test2".to_owned(),
            IceBucketVolumeType::File(IceBucketFileVolume {
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
        let all_databases = ms.list_databases().await.expect("list databases failed");

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

        let no_db_result = ms
            .create_schema(&schema.ident.clone(), schema.clone())
            .await;

        let volume = IceBucketVolume::new("test".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume(&"testv1".to_owned(), volume)
            .await
            .expect("create volume failed");
        ms.create_database(
            &"testdb".to_owned(),
            IceBucketDatabase {
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
            .list_schemas(&schema.ident.database)
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
            .list_schemas(&schema.ident.database)
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

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(
                StructType::builder()
                    .fields(vec![
                        StructField::new(0, "id", true, Type::Primitive(PrimitiveType::Int), None),
                        StructField::new(
                            1,
                            "name",
                            true,
                            Type::Primitive(PrimitiveType::String),
                            None,
                        ),
                    ])
                    .build()
                    .expect("struct type build failed"),
            )
            .build()
            .expect("schema build failed");

        let table = IceBucketTableCreateRequest {
            ident: IceBucketTableIdent {
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

        let volume = IceBucketVolume::new("testv1".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume(&"testv1".to_owned(), volume)
            .await
            .expect("create volume failed");
        ms.create_database(
            &"testdb".to_owned(),
            IceBucketDatabase {
                ident: "testdb".to_owned(),
                volume: "testv1".to_owned(),
                properties: None,
            },
        )
        .await
        .expect("create database failed");
        ms.create_schema(
            &IceBucketSchemaIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
            },
            IceBucketSchema {
                ident: IceBucketSchemaIdent {
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
            .list_tables(&table.ident.clone().into())
            .await
            .expect("list tables failed");
        let table_get = ms.get_table(&table.ident).await.expect("get table failed");
        ms.delete_table(&table.ident, false)
            .await
            .expect("delete table failed");
        let table_list_after = ms
            .list_tables(&table.ident.into())
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

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(
                StructType::builder()
                    .fields(vec![
                        StructField::new(0, "id", true, Type::Primitive(PrimitiveType::Int), None),
                        StructField::new(
                            1,
                            "name",
                            true,
                            Type::Primitive(PrimitiveType::String),
                            None,
                        ),
                    ])
                    .build()
                    .expect("struct type build failed"),
            )
            .build()
            .expect("schema build failed");

        let table = IceBucketTableCreateRequest {
            ident: IceBucketTableIdent {
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

        let volume = IceBucketVolume::new("testv1".to_owned(), IceBucketVolumeType::Memory);
        ms.create_volume(&"testv1".to_owned(), volume)
            .await
            .expect("create volume failed");
        ms.create_database(
            &"testdb".to_owned(),
            IceBucketDatabase {
                ident: "testdb".to_owned(),
                volume: "testv1".to_owned(),
                properties: None,
            },
        )
        .await
        .expect("create database failed");
        ms.create_schema(
            &IceBucketSchemaIdent {
                database: "testdb".to_owned(),
                schema: "testschema".to_owned(),
            },
            IceBucketSchema {
                ident: IceBucketSchemaIdent {
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
