use super::super::models::error::{self as model_error, NexusResult};
use crate::http::ui::models::database::Database;
use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::{Statistics, Table};
use crate::http::ui::models::warehouse::Warehouse;
use crate::state::AppState;
use catalog::models::{Database as DatabaseModel, DatabaseIdent, TableIdent, WarehouseIdent};
use control_plane::models::Warehouse as WarehouseModel;
use snafu::ResultExt;
use uuid::Uuid;

impl AppState {
    pub async fn get_warehouse_model(&self, warehouse_id: Uuid) -> NexusResult<WarehouseModel> {
        self.control_svc
            .get_warehouse(warehouse_id)
            .await
            .context(model_error::WarehouseFetchSnafu { id: warehouse_id })
    }

    pub async fn get_warehouse_by_id(&self, warehouse_id: Uuid) -> NexusResult<Warehouse> {
        self.get_warehouse_model(warehouse_id)
            .await
            .map(std::convert::Into::into)
    }

    pub async fn get_profile_by_id(&self, storage_profile_id: Uuid) -> NexusResult<StorageProfile> {
        self.control_svc
            .get_profile(storage_profile_id)
            .await
            .context(model_error::StorageProfileFetchSnafu {
                id: storage_profile_id,
            })
            .map(std::convert::Into::into)
    }

    pub async fn get_database(&self, ident: &DatabaseIdent) -> NexusResult<Database> {
        self.catalog_svc
            .get_namespace(ident)
            .await
            .context(model_error::DatabaseFetchSnafu { id: ident.clone() })
            .map(std::convert::Into::into)
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::as_conversions,
        clippy::cast_possible_wrap
    )]
    pub async fn list_warehouses(&self) -> NexusResult<Vec<Warehouse>> {
        let warehouses: Vec<Warehouse> = self
            .control_svc
            .list_warehouses()
            .await
            .context(model_error::WarehouseListSnafu)?
            .into_iter()
            .map(std::convert::Into::into)
            .collect();

        let mut result = Vec::new();
        for mut warehouse in warehouses {
            let profile = self.get_profile_by_id(warehouse.storage_profile_id).await?;

            let databases = self.list_databases(warehouse.id, profile.clone()).await?;

            let mut total_statistics = Statistics {
                database_count: Some(databases.len() as i32),
                ..Default::default()
            };
            for database in &databases {
                let stats = database.clone().statistics;
                total_statistics = total_statistics.aggregate(&stats);
            }
            warehouse.storage_profile = profile;
            warehouse.databases = databases;
            warehouse.statistics = total_statistics;
            result.push(warehouse);
        }
        Ok(result)
    }

    pub async fn list_databases_models(
        &self,
        warehouse_id: Uuid,
    ) -> NexusResult<Vec<DatabaseModel>> {
        self.catalog_svc
            .list_namespaces(&WarehouseIdent::new(warehouse_id), None)
            .await
            .context(model_error::DatabaseModelListSnafu { id: warehouse_id })
    }
    pub async fn list_databases(
        &self,
        warehouse_id: Uuid,
        profile: StorageProfile,
    ) -> NexusResult<Vec<Database>> {
        let ident = &WarehouseIdent::new(warehouse_id);
        let databases = self
            .catalog_svc
            .list_namespaces(ident, None)
            .await
            .context(model_error::NamespaceListSnafu { id: warehouse_id })?;

        let mut database_entities = Vec::new();
        for database in databases {
            let tables = self.list_tables(&database.ident).await?;
            let mut entity = Database::from(database);
            entity.with_details(warehouse_id, &profile, tables);
            database_entities.push(entity);
        }
        Ok(database_entities)
    }

    pub async fn list_tables(&self, ident: &DatabaseIdent) -> NexusResult<Vec<Table>> {
        let tables = self
            .catalog_svc
            .list_tables(ident)
            .await
            .context(model_error::TableListSnafu { id: ident.clone() })?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(tables)
    }

    pub async fn get_table(&self, ident: &TableIdent) -> NexusResult<Table> {
        let table = self
            .catalog_svc
            .load_table(ident)
            .await
            .context(model_error::TableFetchSnafu { id: ident.clone() })?;
        Ok(table.into())
    }
}
