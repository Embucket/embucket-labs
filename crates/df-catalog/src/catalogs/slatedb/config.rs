use crate::catalogs::slatedb::databases::DatabasesViewBuilder;
use crate::catalogs::slatedb::volumes::VolumesViewBuilder;
use core_metastore::Metastore;
use core_utils::scan_iterator::ScanIterator;
use datafusion_common::DataFusionError;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SlateDBViewConfig {
    pub database: String,
    pub metastore: Arc<dyn Metastore>,
}

impl SlateDBViewConfig {
    pub async fn make_volumes(
        &self,
        builder: &mut VolumesViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let volumes = self
            .metastore
            .iter_volumes()
            .collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("failed to get volumes: {e}")))?;
        for volume in volumes {
            builder.add_volume(&volume.ident, volume.volume.to_string());
        }
        Ok(())
    }

    pub async fn make_databases(
        &self,
        builder: &mut DatabasesViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let databases = self
            .metastore
            .iter_databases()
            .collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("failed to get databases: {e}")))?;
        for database in databases {
            builder.add_database(database.ident.as_str(), &database.volume);
        }
        Ok(())
    }
}
