use crate::catalogs::slatedb::databases::DatabasesViewBuilder;
use crate::catalogs::slatedb::schemas::SchemasViewBuilder;
use crate::catalogs::slatedb::tables::TablesViewBuilder;
use crate::catalogs::slatedb::volumes::VolumesViewBuilder;
use core_metastore::{Metastore, SchemaIdent};
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
    pub async fn make_schemas(
        &self,
        builder: &mut SchemasViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let schemas = self
            .metastore
            .iter_schemas(&String::new())
            .collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("failed to get schemas: {e}")))?;
        for schema in schemas {
            builder.add_schema(
                &schema.ident.schema,
                &schema.ident.database,
                schema.created_at.to_string(),
                schema.updated_at.to_string(),
            );
        }
        Ok(())
    }
    pub async fn make_tables(
        &self,
        builder: &mut TablesViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let tables = self
            .metastore
            .iter_tables(&SchemaIdent::default())
            .collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("failed to get tables: {e}")))?;
        for table in tables {
            builder.add_tables(
                &table.ident.table,
                &table.ident.schema,
                &table.ident.database,
                table.volume_ident.clone(),
                table.is_temporary,
                table.format.to_string(),
                table.created_at.to_string(),
                table.updated_at.to_string(),
            );
        }
        Ok(())
    }
}
