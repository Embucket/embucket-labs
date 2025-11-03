use crate::catalogs::slatedb::databases::DatabasesViewBuilder;
use crate::catalogs::slatedb::schemas::SchemasViewBuilder;
use crate::catalogs::slatedb::tables::TablesViewBuilder;
use crate::catalogs::slatedb::volumes::VolumesViewBuilder;
use crate::df_error;
use core_metastore::{Metastore, RwObject, SchemaIdent, Volume};
use core_utils::scan_iterator::ScanIterator;
use datafusion_common::DataFusionError;
use snafu::ResultExt;
use std::sync::Arc;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct MetastoreViewConfig {
    pub database: String,
    pub metastore: Arc<dyn Metastore>,
}

impl MetastoreViewConfig {
    #[tracing::instrument(
        name = "MetastoreViewConfig::make_volumes",
        level = "debug",
        skip(self, builder),
        err
    )]
    pub async fn make_volumes(
        &self,
        builder: &mut VolumesViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let volumes = self
            .metastore
            .get_volumes()
            .await
            .context(df_error::MetastoreSnafu)?;
        for volume in volumes {
            builder.add_volume(
                &volume.ident,
                volume.volume.to_string(),
                volume.created_at.to_string(),
                volume.updated_at.to_string(),
            );
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "MetastoreViewConfig::make_databases",
        level = "debug",
        skip(self, builder),
        err
    )]
    pub async fn make_databases(
        &self,
        builder: &mut DatabasesViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let databases = self
            .metastore
            .get_databases(None)
            .await
            .context(df_error::MetastoreSnafu)?;
        let mut volumes: HashMap<i64, RwObject<Volume>> = HashMap::new();
        for database in databases {
            let volume_name = if let Some(volume) = volumes.get(&database.volume_id) {
                volume.ident.clone()
            } else {
                let volume = self.metastore
                    .get_volume_by_id(database.volume_id)
                    .await
                    .context(df_error::MetastoreSnafu)?;
                let volume_ident = volume.ident.clone();
                volumes.insert(database.volume_id, volume);
                volume_ident
            };
            builder.add_database(
                database.ident.as_str(),
                volume_name,
                database.created_at.to_string(),
                database.updated_at.to_string(),
            );
        }
        Ok(())
    }
    #[tracing::instrument(
        name = "MetastoreViewConfig::make_schemas",
        level = "debug",
        skip(self, builder),
        err
    )]
    pub async fn make_schemas(
        &self,
        builder: &mut SchemasViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let schemas = self
            .metastore
            .iter_schemas(&String::new())
            .collect()
            .await
            .context(df_error::CoreUtilsSnafu)?;
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
    #[tracing::instrument(
        name = "MetastoreViewConfig::make_tables",
        level = "debug",
        skip(self, builder),
        err
    )]
    pub async fn make_tables(
        &self,
        builder: &mut TablesViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let tables = self
            .metastore
            .iter_tables(&SchemaIdent::default())
            .collect()
            .await
            .context(df_error::CoreUtilsSnafu)?;
        for table in tables {
            let total_bytes =
                if let Ok(Some(latest_snapshot)) = table.metadata.current_snapshot(None) {
                    latest_snapshot
                        .summary()
                        .other
                        .get("total-files-size")
                        .and_then(|value| value.parse::<i64>().ok())
                        .unwrap_or(0)
                } else {
                    0
                };
            let total_rows =
                if let Ok(Some(latest_snapshot)) = table.metadata.current_snapshot(None) {
                    latest_snapshot
                        .summary()
                        .other
                        .get("total-records")
                        .and_then(|value| value.parse::<i64>().ok())
                        .unwrap_or(0)
                } else {
                    0
                };
            builder.add_tables(
                &table.ident.table,
                &table.ident.schema,
                &table.ident.database,
                table.volume_ident.clone(),
                //TODO: Owner
                None::<String>,
                //TODO: can we add views? If so we need to use TableType enum
                if table.is_temporary {
                    "TEMPORARY".to_string()
                } else {
                    "TABLE".to_string()
                },
                table.format.to_string(),
                total_bytes,
                total_rows,
                table.created_at.to_string(),
                table.updated_at.to_string(),
            );
        }
        Ok(())
    }
}
