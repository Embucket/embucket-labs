use std::sync::Arc;
use crate::{
    error::Result,
    models::{
        RwObject,
        database::{Database, DatabaseIdent},
        schema::{Schema, SchemaIdent},
        table::{Table, TableCreateRequest, TableIdent, TableUpdate},
        volumes::{Volume, VolumeIdent},
    },
};
use async_trait::async_trait;
use core_utils::scan_iterator::VecScanIterator;
use object_store::ObjectStore;
use crate::list_parameters::ListParams;

#[async_trait]
pub trait Metastore: std::fmt::Debug + Send + Sync {
    async fn get_volumes(&self) -> Result<Vec<RwObject<Volume>>>;
    async fn create_volume(&self, volume: Volume) -> Result<RwObject<Volume>>;
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>>;
    async fn get_volume_by_id(&self, id: i64) -> Result<RwObject<Volume>>;
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>>;
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()>;
    async fn volume_object_store(&self, volume_id: i64) -> Result<Option<Arc<dyn ObjectStore>>>;

    async fn get_databases(&self, params: ListParams) -> Result<Vec<RwObject<Database>>>;
    async fn create_database(
        &self,
        database: Database,
    ) -> Result<RwObject<Database>>;
    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>>;
    async fn update_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>>;
    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> Result<()>;

    fn iter_schemas(&self, database: &str) -> VecScanIterator<RwObject<Schema>>;
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
