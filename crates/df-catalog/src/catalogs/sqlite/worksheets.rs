use crate::catalogs::sqlite::history_store_config::HistoryStoreViewConfig;
use core_history::Worksheet;
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
pub struct WorksheetsView {
    schema: SchemaRef,
    config: HistoryStoreViewConfig,
}

impl WorksheetsView {
    pub(crate) fn new(config: HistoryStoreViewConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, true),
            Field::new("created_at", DataType::Utf8, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> WorksheetsViewBuilder {
        WorksheetsViewBuilder {
            worksheet_ids: Int64Builder::new(),
            worksheet_names: StringBuilder::new(),
            content_values: StringBuilder::new(),
            created_at_timestamps: StringBuilder::new(),
            updated_at_timestamps: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for WorksheetsView {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move {
                config.make_worksheets(&mut builder).await?;
                Ok(builder.finish()?)
            }),
        ))
    }
}

pub struct WorksheetsViewBuilder {
    schema: SchemaRef,
    worksheet_ids: Int64Builder,
    worksheet_names: StringBuilder,
    content_values: StringBuilder,
    created_at_timestamps: StringBuilder,
    updated_at_timestamps: StringBuilder,
}

impl WorksheetsViewBuilder {
    pub fn add_worksheet(&mut self, worksheet: Worksheet) {
        // Note: append_value is actually infallible.
        self.worksheet_ids.append_value(worksheet.id);
        self.worksheet_names
            .append_option(if worksheet.name.is_empty() {
                None
            } else {
                Some(worksheet.name)
            });
        self.content_values
            .append_option(if worksheet.content.is_empty() {
                None
            } else {
                Some(worksheet.content)
            });
        self.created_at_timestamps
            .append_value(worksheet.created_at.to_string());
        self.updated_at_timestamps
            .append_value(worksheet.updated_at.to_string());
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.worksheet_ids.finish()),
                Arc::new(self.worksheet_names.finish()),
                Arc::new(self.content_values.finish()),
                Arc::new(self.created_at_timestamps.finish()),
                Arc::new(self.updated_at_timestamps.finish()),
            ],
        )
    }
}
