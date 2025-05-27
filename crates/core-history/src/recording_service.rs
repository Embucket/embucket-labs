use crate::{ExecutionQueryRecord, QueryRecord, WorksheetsStore};
use bytes::Bytes;
use core_executor::utils::{DataSerializationFormat, convert_record_batches};
use core_executor::{
    error::ExecutionResult, models::QueryResult, query::QueryContext, service::ExecutionService,
    session::UserSession,
};
use core_metastore::TableIdent as MetastoreTableIdent;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::reader::Format;
use datafusion::arrow::json::{WriterBuilder, writer::JsonArray};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use snafu::prelude::*;
use std::sync::Arc;
use utoipa::ToSchema;
//
// TODO: This module is pending a rewrite
// TODO: simplify query function
// Is it possible to relax dependency on executor somehow such that history crate is not dependent on executor crate?
pub struct RecordingExecutionService {
    pub execution: Arc<dyn ExecutionService>,
    pub store: Arc<dyn WorksheetsStore>,
    pub data_format: DataSerializationFormat,
}

//TODO: add tests
impl RecordingExecutionService {
    pub fn new(
        execution: Arc<dyn ExecutionService>,
        store: Arc<dyn WorksheetsStore>,
        data_format: DataSerializationFormat,
    ) -> Self {
        Self {
            execution,
            store,
            data_format,
        }
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
    ) -> ExecutionResult<QueryResult> {
        let mut query_record = QueryRecord::query_start(query, query_context.worksheet_id);
        let query_res = self.execution.query(session_id, query, query_context).await;
        match query_res {
            Ok(ref res) => {
                let result_set = ResultSet::query_result_to_result_set(res, self.data_format);
                match result_set {
                    Ok(result_set) => {
                        let encoded_res = serde_json::to_string(&result_set);

                        if let Ok(encoded_res) = encoded_res {
                            let result_count = i64::try_from(res.records.len()).unwrap_or(0);
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
        query_res.map(|q| q.with_query_id(query_record.id))
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    pub r#type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = Row, value_type = Vec<Value>)]
pub struct Row(Vec<Value>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Snafu)]
pub enum ResultSetError {
    #[snafu(display("Failed to create result set: {source}"))]
    CreateResultSet {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(transparent)]
    Execution {
        source: core_executor::error::ExecutionError,
    },
    #[snafu(display("Failed to convert to utf8: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },
    #[snafu(display("Failed to parse result: {source}"))]
    ResultParse { source: serde_json::Error },
}

impl ResultSet {
    pub fn query_result_to_result_set(
        query_result: &QueryResult,
        data_format: DataSerializationFormat,
    ) -> Result<Self, ResultSetError> {
        let buf = Vec::new();
        let write_builder = WriterBuilder::new().with_explicit_nulls(true);
        let mut writer = write_builder.build::<_, JsonArray>(buf);

        // Add columns dbt metadata to each field
        // Since we have to store already converted data to history
        // TODO Perhaps this can be moved closer to Snowflake API layer
        let records = convert_record_batches(query_result.clone(), data_format)
            .map_err(|e| ResultSetError::Execution { source: e })?;
        // serialize records to str
        let records: Vec<&RecordBatch> = records.iter().collect();
        writer
            .write_batches(&records)
            .context(CreateResultSetSnafu)?;
        writer.finish().context(CreateResultSetSnafu)?;

        // Get the underlying buffer back,
        let buf = writer.into_inner();
        let record_batch_str = String::from_utf8(buf).context(Utf8Snafu)?;

        // convert to array, leaving only values
        let rows: Vec<IndexMap<String, Value>> =
            serde_json::from_str(record_batch_str.as_str()).context(ResultParseSnafu)?;
        let rows: Vec<Row> = rows
            .into_iter()
            .map(|obj| Row(obj.values().cloned().collect()))
            .collect();

        let columns = query_result
            .column_info()
            .iter()
            .map(|ci| Column {
                name: ci.name.clone(),
                r#type: ci.r#type.clone(),
            })
            .collect();

        Ok(Self { columns, rows })
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
        let execution_svc = Arc::new(CoreExecutionService::new(metastore.clone()));
        let execution_svc = RecordingExecutionService::new(
            execution_svc.clone(),
            history_store.clone(),
            DataSerializationFormat::Json,
        );

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
