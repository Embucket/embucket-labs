use crate::http::ui::models::database::Database;
use crate::http::ui::models::errors::AppError;
use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::warehouse::Warehouse;
use crate::state::AppState;
use catalog::models::{DatabaseIdent, Table};
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
        self.get_warehouse_model(warehouse_id).await.map(|warehouse| warehouse.into())
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
            }).map(|profile| profile.into())
    }

    pub async fn get_database_by_ident(&self, ident: &DatabaseIdent) -> Result<Database, AppError> {
        self.catalog_svc
            .get_namespace(ident)
            .await
            .map_err(|e| {
                let fmt = format!("{}: failed to get database with db ident {}", e, &ident);
                AppError::new(e, fmt.as_str())
            }).map(|database| database.into())
    }

    pub async fn list_tables(&self, ident: &DatabaseIdent) -> Result<Vec<Table>, AppError> {
        self.catalog_svc.list_tables(ident).await.map_err(|e| {
            let fmt = format!(
                "{}: failed to get database tables with db ident {}",
                e, &ident
            );
            AppError::new(e, fmt.as_str())
        })
    }
}
