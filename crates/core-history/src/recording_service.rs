use crate::{ExecutionQueryRecord, QueryRecord, WorksheetsStore};
use api_structs::result_set::ResultSet;
use bytes::Bytes;
use core_executor::{
    error::ExecutionResult, models::QueryResultData, query::QueryContext,
    service::ExecutionService, session::UserSession, utils::Config,
};
use core_metastore::TableIdent as MetastoreTableIdent;
use datafusion::arrow::csv::reader::Format;
use std::sync::Arc;

//
// TODO: This module is pending a rewrite
// TODO: simplify query function
// Is it possible to relax dependency on executor somehow such that history crate is not dependent on executor crate?
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
    async fn create_session(&self, session_id: String) -> ExecutionResult<Arc<UserSession>> {
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
    ) -> ExecutionResult<QueryResultData> {
        let mut query_record = QueryRecord::query_start(query, query_context.worksheet_id);
        let query_res = self.execution.query(session_id, query, query_context).await;
        match query_res {
            Ok(QueryResultData {
                ref records,
                ref columns_info,
                ..
            }) => {
                let result_set = ResultSet::query_result_to_result_set(records, columns_info);
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
        query_res.map(
            |QueryResultData {
                 records,
                 columns_info,
                 ..
             }: QueryResultData| QueryResultData {
                records,
                columns_info,
                query_id: query_record.id,
            },
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recording_service::RecordingExecutionService;
    use crate::{GetQueries, SlateDBWorksheetsStore, Worksheet, WorksheetsStore};
    use core_executor::service::CoreExecutionService;
    use core_executor::utils::DataSerializationFormat;
    use core_metastore::Metastore;
    use core_metastore::SlateDBMetastore;
    use core_metastore::{Database as MetastoreDatabase, Volume as MetastoreVolume};
    use core_utils::Db;
    use std::sync::Arc;

    #[tokio::test]
    #[allow(clippy::expect_used, clippy::too_many_lines)]
    async fn test_recording_service() {
        let db = Db::memory().await;
        let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
        let history_store = Arc::new(SlateDBWorksheetsStore::new(db));
        let execution_svc = Arc::new(CoreExecutionService::new(
            metastore.clone(),
            Config {
                dbt_serialization_format: DataSerializationFormat::Json,
            },
        ));
        let execution_svc =
            RecordingExecutionService::new(execution_svc.clone(), history_store.clone());

        metastore
            .create_volume(
                &"test_volume".to_string(),
                MetastoreVolume::new(
                    "test_volume".to_string(),
                    core_metastore::VolumeType::Memory,
                ),
            )
            .await
            .expect("Failed to create volume");

        let database_name = "embucket".to_string();

        metastore
            .create_database(
                &database_name.clone(),
                MetastoreDatabase {
                    ident: "embucket".to_string(),
                    properties: None,
                    volume: "test_volume".to_string(),
                },
            )
            .await
            .expect("Failed to create database");

        let session_id = "test_session_id";
        execution_svc
            .create_session(session_id.to_string())
            .await
            .expect("Failed to create session");

        let schema_name = "public".to_string();

        let context =
            QueryContext::new(Some(database_name.clone()), Some(schema_name.clone()), None);

        //Good query
        execution_svc
            .query(
                session_id,
                format!(
                    "CREATE SCHEMA {}.{}",
                    database_name.clone(),
                    schema_name.clone()
                )
                .as_str(),
                context.clone(),
            )
            .await
            .expect("Failed to add schema");

        assert_eq!(
            1,
            history_store
                .get_queries(GetQueries::default())
                .await
                .expect("Failed to get queries")
                .len()
        );

        //Failing query
        execution_svc
            .query(
                session_id,
                format!(
                    "CREATE SCHEMA {}.{}",
                    database_name.clone(),
                    schema_name.clone()
                )
                .as_str(),
                context.clone(),
            )
            .await
            .expect_err("Failed to not add schema");

        assert_eq!(
            2,
            history_store
                .get_queries(GetQueries::default())
                .await
                .expect("Failed to get queries")
                .len()
        );

        let table_name = "test1".to_string();

        //Create table queries
        execution_svc
            .query(
                session_id,
                format!(
                    "create TABLE {}.{}.{}
        external_volume = ''
	    catalog = ''
	    base_location = ''
        (
	    APP_ID TEXT,
	    PLATFORM TEXT,
	    EVENT TEXT,
        TXN_ID NUMBER(38,0),
        EVENT_TIME TEXT
	    );",
                    database_name.clone(),
                    schema_name.clone(),
                    table_name.clone()
                )
                .as_str(),
                context.clone(),
            )
            .await
            .expect("Failed to create table");

        assert_eq!(
            3,
            history_store
                .get_queries(GetQueries::default())
                .await
                .expect("Failed to get queries")
                .len()
        );

        //Insert into query
        execution_svc
            .query(
                session_id,
                format!(
                    "INSERT INTO {}.{}.{} (APP_ID, PLATFORM, EVENT, TXN_ID, EVENT_TIME)
        VALUES ('12345', 'iOS', 'login', '123456', '2021-01-01T00:00:00'),
               ('67890', 'Android', 'purchase', '456789', '2021-01-01T00:02:00')",
                    database_name.clone(),
                    schema_name.clone(),
                    table_name.clone()
                )
                .as_str(),
                context.clone(),
            )
            .await
            .expect("Failed to insert into");

        assert_eq!(
            4,
            history_store
                .get_queries(GetQueries::default())
                .await
                .expect("Failed to get queries")
                .len()
        );

        //With worksheet
        let worksheet = history_store
            .add_worksheet(Worksheet::new("Testing1".to_string(), String::new()))
            .await
            .expect("Failed to add worksheet");

        assert_eq!(
            0,
            history_store
                .get_queries(GetQueries::default().with_worksheet_id(worksheet.clone().id))
                .await
                .expect("Failed to get queries")
                .len()
        );

        execution_svc
            .query(
                session_id,
                format!(
                    "INSERT INTO {}.{}.{} (APP_ID, PLATFORM, EVENT, TXN_ID, EVENT_TIME)
        VALUES ('1234', 'iOS', 'login', '123456', '2021-01-01T00:00:00'),
               ('6789', 'Android', 'purchase', '456789', '2021-01-01T00:02:00')",
                    database_name.clone(),
                    schema_name.clone(),
                    table_name.clone()
                )
                .as_str(),
                QueryContext::new(
                    Some(database_name.clone()),
                    Some(schema_name.clone()),
                    Some(worksheet.clone().id),
                ),
            )
            .await
            .expect("Failed to insert into");

        assert_eq!(
            1,
            history_store
                .get_queries(GetQueries::default().with_worksheet_id(worksheet.clone().id))
                .await
                .expect("Failed to get queries")
                .len()
        );
    }
}
