use crate::execution::error::ExecutionResult;
use crate::execution::models::ColumnInfo;
use crate::execution::query::QueryContext;
use crate::execution::service::ExecutionService;
use crate::execution::utils::Config;
use crate::http::ui::queries::models::ResultSet;
use arrow::csv::reader::Format;
use arrow_array::RecordBatch;
use bytes::Bytes;
use embucket_history::{QueryRecord, QueryRecordActions, WorksheetsStore};
use embucket_metastore::TableIdent as MetastoreTableIdent;
use std::sync::Arc;

pub struct RecordingExecutionService {
    pub execution: Arc<dyn ExecutionService>,
    pub store: Arc<dyn WorksheetsStore>,
}

//TODO: add tests
impl RecordingExecutionService {
    pub fn new(execution: Arc<dyn ExecutionService>, store: Arc<dyn WorksheetsStore>) -> Self {
        Self { execution, store }
    }
}

#[async_trait::async_trait]
impl ExecutionService for RecordingExecutionService {
    async fn create_session(&self, session_id: String) -> ExecutionResult<()> {
        self.execution.create_session(session_id).await
    }

    async fn delete_session(&self, session_id: String) -> ExecutionResult<()> {
        self.execution.delete_session(session_id).await
    }

    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> ExecutionResult<(Vec<RecordBatch>, Vec<ColumnInfo>)> {
        let mut query_record = QueryRecord::query_start(query, query_context.worksheet_id);
        let query_res = self.execution.query(session_id, query, query_context).await;
        match query_res {
            Ok((ref records, ref columns)) => {
                let result_set = ResultSet::query_result_to_result_set(records, columns);
                match result_set {
                    Ok(result_set) => {
                        let encoded_res = serde_json::to_string(&result_set);

                        if let Ok(encoded_res) = encoded_res {
                            let result_count = i64::try_from(records.len()).unwrap_or(0);
                            query_record.query_finished(result_count, Some(encoded_res));
                        }
                        // failed to wrap query results
                        else if let Err(err) = encoded_res {
                            query_record.query_finished_with_error(err.to_string());
                        }
                    }
                    // error getting result_set
                    Err(err) => {
                        query_record.query_finished_with_error(err.to_string());
                    }
                }
            }
            // query error
            Err(ref err) => {
                // query execution error
                query_record.query_finished_with_error(err.to_string());
            }
        }
        // add query record
        if let Err(err) = self.store.add_query(&query_record).await {
            // do not raise error, just log ?
            tracing::error!("{err}");
        }
        query_res
    }

    async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &MetastoreTableIdent,
        data: Bytes,
        file_name: &str,
        format: Format,
    ) -> ExecutionResult<usize> {
        self.execution
            .upload_data_to_table(session_id, table_ident, data, file_name, format)
            .await
    }

    fn config(&self) -> &Config {
        self.execution.config()
    }
}
