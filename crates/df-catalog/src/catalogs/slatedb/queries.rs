use crate::catalogs::slatedb::history_store_config::HistoryStoreViewConfig;
use core_history::QueryRecord;
use datafusion::arrow::array::Int64Builder;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
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
                Ok(builder.finish()?)
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
    pub fn add_query(&mut self, query_record: QueryRecord) {
        // Note: append_value is actually infallible.
        self.query_ids.append_value(query_record.id.into());
        self.worksheet_ids.append_option(query_record.worksheet_id);
        self.queries.append_value(query_record.query);
        self.start_time_timestamps
            .append_value(query_record.start_time.to_string());
        self.end_time_timestamps
            .append_value(query_record.end_time.to_string());
        self.duration_ms_values
            .append_value(query_record.duration_ms);
        self.result_count_values
            .append_value(query_record.result_count);
        self.results.append_option(query_record.result);
        self.statuses.append_value(query_record.status.to_string());
        self.errors.append_option(query_record.error);
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
