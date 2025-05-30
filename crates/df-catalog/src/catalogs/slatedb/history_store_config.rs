use crate::catalogs::slatedb::queries::QueriesViewBuilder;
use crate::catalogs::slatedb::worksheets::WorksheetsViewBuilder;
use core_history::{GetQueriesParams, HistoryStore};
use datafusion_common::DataFusionError;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct HistoryStoreViewConfig {
    pub database: String,
    pub history_store: Arc<dyn HistoryStore>,
}

impl HistoryStoreViewConfig {
    pub async fn make_worksheets(
        &self,
        builder: &mut WorksheetsViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let worksheets =
            self.history_store.get_worksheets().await.map_err(|e| {
                DataFusionError::Execution(format!("failed to get worksheets: {e}"))
            })?;
        for worksheet in worksheets {
            builder.add_worksheet(
                worksheet.id,
                if worksheet.name.is_empty() {
                    None
                } else {
                    Some(worksheet.name)
                },
                if worksheet.content.is_empty() {
                    None
                } else {
                    Some(worksheet.content)
                },
                worksheet.created_at.to_string(),
                worksheet.updated_at.to_string(),
            );
        }
        Ok(())
    }

    pub async fn make_queries(
        &self,
        builder: &mut QueriesViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let queries = self
            .history_store
            .get_queries(GetQueriesParams::default())
            .await
            .map_err(|e| DataFusionError::Execution(format!("failed to get queries: {e}")))?;
        for query in queries {
            builder.add_query(
                query.id,
                query.worksheet_id,
                query.query,
                query.start_time.to_string(),
                query.end_time.to_string(),
                query.duration_ms,
                query.result_count,
                query.result,
                query.status.to_string(),
                query.error,
            );
        }
        Ok(())
    }
}
