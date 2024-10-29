use crate::http::ui::models::database::Database;
use crate::http::ui::models::errors::AppError;
use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::{Statistics, Table};
use crate::http::ui::models::warehouse::Warehouse;
use crate::state::AppState;
use catalog::models::{DatabaseIdent, TableIdent, WarehouseIdent};
use control_plane::models::Warehouse as WarehouseModel;
use uuid::Uuid;

impl AppState {
    pub async fn get_warehouse_model(
        &self,
        warehouse_id: Uuid,
    ) -> Result<WarehouseModel, AppError> {
        self.control_svc
            .get_warehouse(warehouse_id)
            .await
            .map_err(|e| {
                let fmt = format!("{}: failed to get warehouse by id {}", e, warehouse_id);
                AppError::new(e, fmt.as_str())
            })
    }
    pub async fn get_warehouse_by_id(&self, warehouse_id: Uuid) -> Result<Warehouse, AppError> {
        self.get_warehouse_model(warehouse_id)
            .await
            .map(|warehouse| warehouse.into())
    }

    pub async fn get_profile_by_id(
        &self,
        storage_profile_id: Uuid,
    ) -> Result<StorageProfile, AppError> {
        self.control_svc
            .get_profile(storage_profile_id)
            .await
            .map_err(|e| {
                let fmt = format!("{}: failed to get profile by id {}", e, storage_profile_id);
                AppError::new(e, fmt.as_str())
            })
            .map(|profile| profile.into())
    }

    pub async fn get_database(&self, ident: &DatabaseIdent) -> Result<Database, AppError> {
        self.catalog_svc
            .get_namespace(ident)
            .await
            .map_err(|e| {
                let fmt = format!("{}: failed to get database with db ident {}", e, &ident);
                AppError::new(e, fmt.as_str())
            })
            .map(|database| database.into())
    }

    pub async fn list_warehouses(&self) -> Result<Vec<Warehouse>, AppError> {
        let warehouses: Vec<Warehouse> = self
            .control_svc
            .list_warehouses()
            .await
            .map_err(|e| {
                let fmt = format!("{}: failed to get warehouses", e);
                AppError::new(e, fmt.as_str())
            })?
            .into_iter()
            .map(|w| w.into())
            .collect();

        let mut result = Vec::new();
        for mut warehouse in warehouses {
            let databases = self.list_databases(warehouse.id).await?;

            let profile = self
                .get_profile_by_id(warehouse.storage_profile_id.unwrap())
                .await?;

            let mut total_statistics = Statistics::default();
            databases.iter().for_each(|database| {
                let stats = database.clone().statistics.unwrap_or_default();
                total_statistics = total_statistics.aggregate(&stats);
            });

            warehouse.with_details(
                Option::from(profile.clone()),
                Option::from(databases.clone()),
            );
            warehouse.statistics = Some(total_statistics);
            result.push(warehouse)
        }
        Ok(result)
    }
    pub async fn list_databases(&self, warehouse_id: Uuid) -> Result<Vec<Database>, AppError> {
        let ident = &WarehouseIdent::new(warehouse_id);
        let databases = self
            .catalog_svc
            .list_namespaces(ident, None)
            .await
            .map_err(|e| {
                let fmt = format!(
                    "{}: failed to get warehouse databases with wh id {}",
                    e, warehouse_id
                );
                AppError::new(e, fmt.as_str())
            })?;

        let mut database_entities = Vec::new();
        for database in databases {
            let tables = self.catalog_svc.list_tables(&database.ident).await?;
            let mut total_statistics = Statistics::default();

            for table in tables {
                let table_stats = Statistics::from_table_metadata(&table.metadata);
                total_statistics = total_statistics.aggregate(&table_stats);
            }

            total_statistics.database_count = Option::from(1);
            let mut entity = Database::from(database);
            entity.statistics = Option::from(total_statistics);
            database_entities.push(entity);
        }
        Ok(database_entities)
    }

    pub async fn list_tables(&self, ident: &DatabaseIdent) -> Result<Vec<Table>, AppError> {
        let tables = self
            .catalog_svc
            .list_tables(ident)
            .await
            .map_err(|e| {
                let fmt = format!(
                    "{}: failed to get database tables with db ident {}",
                    e, &ident
                );
                AppError::new(e, fmt.as_str())
            })?
            .into_iter()
            .map(|table| table.into())
            .collect();
        Ok(tables)
    }

    pub async fn get_table(&self, ident: &TableIdent) -> Result<Table, AppError> {
        let table = self.catalog_svc.load_table(ident).await.map_err(|e| {
            let fmt = format!(
                "{}: failed to get database tables with db ident {}",
                e, &ident
            );
            AppError::new(e, fmt.as_str())
        })?;
        Ok(table.into())
    }
}
