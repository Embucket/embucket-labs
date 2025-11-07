use std::{collections::HashMap, sync::Arc};

use crate::error::NoIdSnafu;
#[allow(clippy::wildcard_imports)]
use crate::models::*;
use crate::sqlite::crud;
use crate::{
    Metastore,
    error::{self as metastore_err, Result},
    list_parameters::ListParams,
    models::{
        RwObject,
        database::{Database, DatabaseIdent},
        schema::{Schema, SchemaIdent},
        table::{Table, TableCreateRequest, TableIdent, TableUpdate},
        volumes::{Volume, VolumeIdent},
    },
    sqlite::Stats,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use core_sqlite::SqliteDb;
use dashmap::DashMap;
use deadpool_diesel::sqlite::Connection;
use deadpool_diesel::sqlite::{Manager, Pool as DieselPool, Runtime};
use deadpool_sqlite::Object;
use diesel::migration;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use iceberg_rust::catalog::commit::apply_table_updates;
use iceberg_rust_spec::{
    schema::Schema as IcebergSchema,
    table_metadata::{FormatVersion, TableMetadataBuilder},
    types::{StructField, Type},
};
use object_store::{ObjectStore, PutPayload, path::Path};
use snafu::OptionExt;
use snafu::ResultExt;
use strum::Display;
use tokio::sync::RwLock;
use tracing::instrument;
use uuid::Uuid;

pub const SQLITE_METASTORE_DB_NAME: &str = "sqlite_data/metastore.db";

pub const EMBED_MIGRATIONS: EmbeddedMigrations = embed_migrations!("src/sqlite/migrations");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[strum(serialize_all = "lowercase")]
pub enum MetastoreObjectType {
    Volume,
    Database,
    Schema,
}

#[derive(Debug, Default)]
struct MetastoreState {
    tables: HashMap<(DatabaseIdent, String, String), RwObject<Table>>,
}

pub struct MetastoreDb {
    in_memory_state: RwLock<MetastoreState>,
    object_store_cache: DashMap<i64, Arc<dyn ObjectStore>>,
    pub diesel_pool: DieselPool,
    raw_sqls_db: SqliteDb,
}

impl std::fmt::Debug for MetastoreDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metastore").finish()
    }
}

impl MetastoreDb {
    #[allow(clippy::expect_used)]
    pub async fn new() -> Result<Self> {
        if let Some(dir_path) = std::path::Path::new(SQLITE_METASTORE_DB_NAME).parent() {
            std::fs::create_dir_all(dir_path).context(metastore_err::CreateDirSnafu)?;
        }

        // use this machinery just to set pragmas
        // but also use its connection pool for raw sql
        let sqlite_db = SqliteDb::new(SQLITE_METASTORE_DB_NAME)
            .await
            .context(metastore_err::CoreSqliteSnafu)?;

        let metastore = Self {
            in_memory_state: RwLock::new(MetastoreState::default()),
            object_store_cache: DashMap::new(),
            diesel_pool: Self::create_pool(SQLITE_METASTORE_DB_NAME)?,
            raw_sqls_db: sqlite_db,
        };
        metastore.create_tables().await?;
        Ok(metastore)
    }

    // Create a new store with a new in-memory database
    #[allow(clippy::expect_used)]
    pub async fn new_in_memory() -> Self {
        // use unique filename for every test, create in memory database
        let thread = std::thread::current();
        let sqlite_db_name = format!("file:{:?}_meta?mode=memory&cache=shared", thread.id());
        let sqlite_db = SqliteDb::new(&sqlite_db_name)
            .await
            .expect("Failed to create Sqlite Db for metastore");

        let store = Self {
            in_memory_state: RwLock::new(MetastoreState::default()),
            object_store_cache: DashMap::new(),
            diesel_pool: Self::create_pool(&sqlite_db_name)
                .expect("Failed to create Diesel Pool for metastore"),
            raw_sqls_db: sqlite_db,
        };

        store
            .create_tables()
            .await
            .expect("Failed to create tables");
        store
    }

    pub fn create_pool(conn_str: &str) -> Result<DieselPool> {
        let pool = DieselPool::builder(Manager::new(conn_str, Runtime::Tokio1))
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
        let conn = self.connection().await?;
        let migrations = conn
            .interact(|conn| -> migration::Result<Vec<String>> {
                Ok(conn
                    .run_pending_migrations(EMBED_MIGRATIONS)?
                    .iter()
                    .map(ToString::to_string)
                    .collect())
            })
            .await?
            .context(metastore_err::GenericSnafu)?;

        tracing::info!("create_tables using migrations: {migrations:?}");
        Ok(())
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

    async fn connection(&self) -> Result<Connection> {
        self.diesel_pool
            .get()
            .await
            .context(metastore_err::DieselPoolSnafu)
    }

    async fn connection_for_raw_sqls(&self) -> Result<Object> {
        self.raw_sqls_db
            .conn()
            .await
            .context(metastore_err::SqliteDbSnafu)
    }

    async fn table_object_store_from_request(
        &self,
        table: &TableCreateRequest,
        ident: &TableIdent,
    ) -> Result<Arc<dyn ObjectStore>> {
        let volume_ident = table.volume_ident.as_ref().ok_or_else(|| {
            metastore_err::TableVolumeMissingSnafu {
                table: ident.table.clone(),
            }
            .build()
        })?;
        let volume =
            self.get_volume(volume_ident)
                .await?
                .context(metastore_err::VolumeNotFoundSnafu {
                    volume: volume_ident,
                })?;
        self.volume_object_store(volume.id()?)
            .await?
            .ok_or_else(|| {
                metastore_err::VolumeNotFoundSnafu {
                    volume: volume_ident.clone(),
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
        let file_name = format!("{}.metadata.json", Uuid::new_v4());
        let path = format!(
            "{}/{}/{}",
            table.database.to_ascii_lowercase(),
            table.schema.to_ascii_lowercase(),
            file_name
        );
        let bytes = serde_json::to_vec(metadata).context(metastore_err::SerializeMetadataSnafu)?;
        object_store
            .put(
                &Path::from(path.clone()),
                PutPayload::from_bytes(Bytes::from(bytes)),
            )
            .await
            .context(metastore_err::ObjectStoreSnafu)?;
        Ok(path)
    }

    fn table_key(ident: &TableIdent) -> (DatabaseIdent, String, String) {
        (
            ident.database.to_ascii_lowercase(),
            ident.schema.to_ascii_lowercase(),
            ident.table.to_ascii_lowercase(),
        )
    }
}

#[async_trait]
impl Metastore for MetastoreDb {
    #[instrument(name = "SqliteMetastore::get_stats", level = "debug", skip(self), err)]
    async fn get_stats(&self) -> Result<Stats> {
        let connection = self.connection_for_raw_sqls().await?;
        crate::sqlite::get_stats(&connection).await
    }

    #[instrument(
        name = "SqliteMetastore::get_volumes",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_volumes(&self, params: ListParams) -> Result<Vec<RwObject<Volume>>> {
        let conn = self.connection().await?;
        crud::volumes::list_volumes(&conn, params).await
    }

    #[instrument(
        name = "SqliteMetastore::create_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn create_volume(&self, volume: Volume) -> Result<RwObject<Volume>> {
        let conn = self.connection().await?;
        let object_store = volume.get_object_store()?;
        let volume = crud::volumes::create_volume(&conn, RwObject::new(volume)).await?;

        tracing::debug!("Volume {} created", volume.ident);

        self.object_store_cache
            .insert(*volume.id().context(NoIdSnafu)?, object_store);
        Ok(volume)
    }

    #[instrument(name = "SqliteMetastore::get_volume", level = "debug", skip(self), err)]
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>> {
        let conn = self.connection().await?;
        crud::volumes::get_volume(&conn, name).await
    }

    #[instrument(
        name = "SqliteMetastore::get_volume_by_id",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_volume_by_id(&self, id: VolumeId) -> Result<RwObject<Volume>> {
        let conn = self.connection().await?;
        crud::volumes::get_volume_by_id(&conn, id).await
    }

    #[instrument(
        name = "SqliteMetastore::get_volume_by_database",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_volume_by_database(
        &self,
        database: &DatabaseIdent,
    ) -> Result<Option<RwObject<Volume>>> {
        let conn = self.connection().await?;
        crud::volumes::get_volume_by_database(&conn, database.clone()).await
    }

    // TODO: Allow rename only here or on REST API level
    #[instrument(
        name = "SqliteMetastore::update_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn update_volume(&self, ident: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        let conn = self.connection().await?;
        let updated_volume = crud::volumes::update_volume(&conn, ident, volume.clone()).await?;
        let object_store = updated_volume.get_object_store()?;
        // object store cached by id so just alter value
        self.object_store_cache
            .alter(&*updated_volume.id().context(NoIdSnafu)?, |_, _store| {
                object_store.clone()
            });
        Ok(updated_volume)
    }

    #[instrument(
        name = "SqliteMetastore::delete_volume",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()> {
        let conn = self.connection().await?;

        let volume = crud::volumes::get_volume(&conn, name).await?.context(
            metastore_err::VolumeNotFoundSnafu {
                volume: name.to_string(),
            },
        )?;
        let volume_id = volume.id().context(NoIdSnafu)?;
        let db_names =
            crud::databases::list_databases(&conn, ListParams::new().by_parent_id(*volume_id))
                .await?
                .iter()
                .map(|db| db.ident.clone())
                .collect::<Vec<String>>();

        if cascade && !db_names.is_empty() {
            return metastore_err::VolumeInUseSnafu {
                database: db_names.join(", "),
            }
            .fail();
        }

        let _ = crud::volumes::delete_volume_cascade(&conn, name).await?;
        Ok(())
    }

    #[instrument(
        name = "SqliteMetastore::volume_object_store",
        level = "trace",
        skip(self),
        err
    )]
    async fn volume_object_store(
        &self,
        volume_id: VolumeId,
    ) -> Result<Option<Arc<dyn ObjectStore>>> {
        if let Some(store) = self.object_store_cache.get(&*volume_id) {
            Ok(Some(store.clone()))
        } else {
            let volume = self.get_volume_by_id(volume_id).await?;
            let object_store = volume.get_object_store()?;
            self.object_store_cache
                .insert(*volume_id, object_store.clone());
            Ok(Some(object_store))
        }
    }

    #[instrument(name = "SqliteMetastore::get_databases", level = "trace", skip(self))]
    async fn get_databases(&self, params: ListParams) -> Result<Vec<RwObject<Database>>> {
        let conn = self.connection().await?;
        crud::databases::list_databases(&conn, params).await
    }

    #[instrument(
        name = "SqliteMetastore::create_database",
        level = "debug",
        skip(self),
        err
    )]
    async fn create_database(&self, database: Database) -> Result<RwObject<Database>> {
        let conn = self.connection().await?;
        let volume = crud::volumes::get_volume(&conn, &database.volume)
            .await?
            .context(metastore_err::VolumeNotFoundSnafu {
                volume: database.volume.clone(),
            })?;

        let database = RwObject::new(database).with_volume_id(volume.id().context(NoIdSnafu)?);
        let resulted = crud::databases::create_database(&conn, database.clone()).await?;

        tracing::debug!("Created database: {}", resulted.ident);
        Ok(resulted)
    }

    #[instrument(
        name = "SqliteMetastore::get_database",
        level = "trace",
        skip(self),
        err
    )]
    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>> {
        let conn = self.connection().await?;
        crud::databases::get_database(&conn, name).await
    }

    #[instrument(
        name = "SqliteMetastore::update_database",
        level = "debug",
        skip(self, database),
        err
    )]
    // Database can only be renamed, properties updated
    async fn update_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>> {
        let conn = self.connection().await?;
        crud::databases::update_database(&conn, name, database).await
    }

    #[instrument(
        name = "SqliteMetastore::delete_database",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> Result<()> {
        let conn = self.connection().await?;

        let schemas = self
            .get_schemas(ListParams::new().by_parent_name(name.clone()))
            .await?;

        if cascade && !schemas.is_empty() {
            let schemas_names = schemas
                .iter()
                .map(|s| s.ident.schema.clone())
                .collect::<Vec<String>>();

            return metastore_err::DatabaseInUseSnafu {
                database: name,
                schema: schemas_names.join(", "),
            }
            .fail();
        }

        crud::databases::delete_database_cascade(&conn, name).await?;
        Ok(())
    }

    #[instrument(
        name = "SqliteMetastore::get_schemas",
        level = "debug",
        skip(self),
        fields(items)
    )]
    async fn get_schemas(&self, params: ListParams) -> Result<Vec<RwObject<Schema>>> {
        let conn = self.connection().await?;
        let items = crud::schemas::list_schemas(&conn, params).await?;
        tracing::Span::current().record("items", format!("{items:?}"));
        Ok(items)
    }

    #[instrument(
        name = "SqliteMetastore::create_schema",
        level = "debug",
        skip(self, schema),
        err
    )]
    async fn create_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>> {
        let conn = self.connection().await?;
        let database = crud::databases::get_database(&conn, &ident.database)
            .await?
            .context(metastore_err::DatabaseNotFoundSnafu {
                db: ident.database.clone(),
            })?;

        let schema = RwObject::new(schema).with_database_id(database.id().context(NoIdSnafu)?);
        let resulted = crud::schemas::create_schema(&conn, schema.clone()).await?;

        tracing::debug!("Created schema: {}", resulted.ident);
        Ok(resulted)
    }

    #[instrument(name = "SqliteMetastore::get_schema", level = "debug", skip(self), err)]
    async fn get_schema(&self, ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>> {
        let conn = self.connection().await?;
        crud::schemas::get_schema(&conn, ident).await
    }

    #[instrument(
        name = "SqliteMetastore::get_schema_by_id",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<RwObject<Schema>> {
        let conn = self.connection().await?;
        crud::schemas::get_schema_by_id(&conn, id).await
    }

    #[instrument(
        name = "SqliteMetastore::update_schema",
        level = "debug",
        skip(self, schema),
        err
    )]
    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>> {
        let conn = self.connection().await?;
        crud::schemas::update_schema(&conn, ident, schema).await
    }

    #[instrument(
        name = "SqliteMetastore::delete_schema",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_schema(&self, ident: &SchemaIdent, _cascade: bool) -> Result<()> {
        let conn = self.connection().await?;
        let _deleted_schema_id = crud::schemas::delete_schema_cascade(&conn, ident).await?;
        Ok(())
    }

    #[instrument(name = "SqliteMetastore::get_tables", level = "debug", skip(self))]
    async fn get_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>> {
        let in_memory_state = self.in_memory_state.read().await;
        Ok(in_memory_state
            .tables
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, table)| table.clone())
            .collect())
    }

    #[allow(clippy::too_many_lines)]
    #[instrument(
        name = "SqliteMetastore::create_table",
        level = "debug",
        skip(self),
        err
    )]
    #[allow(clippy::too_many_lines)]
    async fn create_table(
        &self,
        ident: &TableIdent,
        mut table: TableCreateRequest,
    ) -> Result<RwObject<Table>> {
        let mut in_memory_state = self.in_memory_state.write().await;
        let schema = ident.schema.clone();
        let database = ident.database.clone();
        if self
            .get_schema(&SchemaIdent { schema, database })
            .await?
            .is_none()
        {
            return metastore_err::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }

        if self.get_table(ident).await?.is_some() {
            return metastore_err::TableAlreadyExistsSnafu {
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
            .context(metastore_err::TableMetadataBuilderSnafu)?;

        if metadata.properties.is_empty() {
            metadata.properties = HashMap::new();
        }

        let object_store = self.table_object_store_from_request(&table, ident).await?;
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
        in_memory_state
            .tables
            .insert(Self::table_key(ident), row.clone());
        Ok(row)
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
        update: TableUpdate,
    ) -> Result<RwObject<Table>> {
        let object_store = self.table_object_store(ident).await?.ok_or_else(|| {
            metastore_err::TableNotFoundSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .build()
        })?;

        let mut in_memory_state = self.in_memory_state.write().await;
        let table_entry = in_memory_state
            .tables
            .get_mut(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_err::TableNotFoundSnafu {
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
            .context(metastore_err::IcebergSnafu)?;

        let metadata_location = self.put_metadata(ident, object_store, &metadata).await?;
        table_entry.data.metadata = metadata;
        table_entry.data.metadata_location = metadata_location;
        table_entry.touch();
        Ok(table_entry.clone())
    }

    #[instrument(
        name = "SqliteMetastore::delete_table",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_table(&self, ident: &TableIdent, _cascade: bool) -> Result<()> {
        let mut in_memory_state = self.in_memory_state.write().await;
        in_memory_state.tables.remove(&Self::table_key(ident));
        Ok(())
    }

    #[instrument(name = "SqliteMetastore::get_table", level = "debug", skip(self))]
    async fn get_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Table>>> {
        let in_memory_state = self.in_memory_state.read().await;
        Ok(in_memory_state.tables.get(&Self::table_key(ident)).cloned())
    }

    #[instrument(
        name = "SqliteMetastore::table_object_store",
        level = "debug",
        skip(self)
    )]
    async fn table_object_store(&self, ident: &TableIdent) -> Result<Option<Arc<dyn ObjectStore>>> {
        if let Some(volume) = self.volume_for_table(ident).await? {
            self.volume_object_store(volume.id().context(NoIdSnafu)?)
                .await
        } else {
            Ok(None)
        }
    }

    #[instrument(name = "SqliteMetastore::table_exists", level = "debug", skip(self))]
    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        self.get_table(ident).await.map(|table| table.is_some())
    }

    #[instrument(
        name = "SqliteMetastore::volume_for_table",
        level = "debug",
        skip(self)
    )]
    async fn volume_for_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Volume>>> {
        let conn = self.connection().await?;
        if let Some(Some(volume_ident)) = self
            .get_table(ident)
            .await?
            .map(|table| table.volume_ident.clone())
        {
            crud::volumes::get_volume(&conn, &volume_ident).await
        } else {
            let database = crud::databases::get_database(&conn, &ident.database)
                .await?
                .context(metastore_err::DatabaseNotFoundSnafu {
                    db: ident.database.clone(),
                })?;
            Ok(Some(
                crud::volumes::get_volume_by_id(&conn, database.volume_id()?).await?,
            ))
        }
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
