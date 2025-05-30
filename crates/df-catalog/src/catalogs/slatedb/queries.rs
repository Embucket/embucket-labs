use crate::catalogs::slatedb::history_store_config::HistoryStoreViewConfig;
use datafusion::arrow::array::Int64Builder;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct QueriesView {
    schema: SchemaRef,
    config: HistoryStoreViewConfig,
}

impl QueriesView {
    pub(crate) fn new(config: HistoryStoreViewConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("worksheet_id", DataType::Int64, true),
            Field::new("query", DataType::Utf8, false),
            Field::new("start_time", DataType::Utf8, false),
            Field::new("end_time", DataType::Utf8, false),
            Field::new("duration_ms", DataType::Int64, false),
            Field::new("result_count", DataType::Int64, false),
            Field::new("result", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, false),
            Field::new("error", DataType::Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> QueriesViewBuilder {
        QueriesViewBuilder {
            query_ids: Int64Builder::new(),
            worksheet_ids: Int64Builder::new(),
            queries: StringBuilder::new(),
            start_time_timestamps: StringBuilder::new(),
            end_time_timestamps: StringBuilder::new(),
            duration_ms_values: Int64Builder::new(),
            result_count_values: Int64Builder::new(),
            results: StringBuilder::new(),
            statuses: StringBuilder::new(),
            errors: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for QueriesView {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move {
                config.make_queries(&mut builder).await?;
                builder
                    .finish()
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            }),
        ))
    }
}

pub struct QueriesViewBuilder {
    schema: SchemaRef,
    query_ids: Int64Builder,
    worksheet_ids: Int64Builder,
    queries: StringBuilder,
    start_time_timestamps: StringBuilder,
    end_time_timestamps: StringBuilder,
    duration_ms_values: Int64Builder,
    result_count_values: Int64Builder,
    results: StringBuilder,
    statuses: StringBuilder,
    errors: StringBuilder,
}

impl QueriesViewBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn add_query(
        &mut self,
        query_id: i64,
        worksheet_id: Option<i64>,
        query: impl AsRef<str>,
        start_time: impl AsRef<str>,
        end_time: impl AsRef<str>,
        duration_ms: i64,
        result_count: i64,
        result: Option<impl AsRef<str>>,
        status: impl AsRef<str>,
        error: Option<impl AsRef<str>>,
    ) {
        // Note: append_value is actually infallible.
        self.query_ids.append_value(query_id);
        self.worksheet_ids.append_option(worksheet_id);
        self.queries.append_value(query.as_ref());
        self.start_time_timestamps.append_value(start_time.as_ref());
        self.end_time_timestamps.append_value(end_time.as_ref());
        self.duration_ms_values.append_value(duration_ms);
        self.result_count_values.append_value(result_count);
        self.results.append_option(result);
        self.statuses.append_value(status.as_ref());
        self.errors.append_option(error);
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.query_ids.finish()),
                Arc::new(self.worksheet_ids.finish()),
                Arc::new(self.queries.finish()),
                Arc::new(self.start_time_timestamps.finish()),
                Arc::new(self.end_time_timestamps.finish()),
                Arc::new(self.duration_ms_values.finish()),
                Arc::new(self.result_count_values.finish()),
                Arc::new(self.results.finish()),
                Arc::new(self.statuses.finish()),
                Arc::new(self.errors.finish()),
            ],
        )
    }
}
